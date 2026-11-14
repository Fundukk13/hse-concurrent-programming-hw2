package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private final ReentrantLock poolStateLock;
    private final Condition evictionScheduleChanged;
    private final Condition shutdownProgressChanged;
    private final ConnectionFactory connectionFactory;
    private final long idleTimeoutNanos;
    private final ArrayDeque<IdleConnection> idleConnections = new ArrayDeque<>();
    private final Set<Lease> activeLeases = new HashSet<>();
    private final ArrayDeque<Waiter> pendingBorrows = new ArrayDeque<>();
    private final Thread idleEvictionThread;
    private final PoolConfig initialConfig;
    private int capacityLimit;
    private int pendingCreations;
    private int pendingCloses;
    private volatile boolean shutdownStarted;

    public ConnectionPoolImpl(PoolConfig initialConfig, ConnectionFactory connectionFactory) {
        this.initialConfig = Objects.requireNonNull(initialConfig, "config");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "factory");
        capacityLimit = initialConfig.maxSize();
        idleTimeoutNanos = toNanos(initialConfig.idleTimeout());
        poolStateLock = new ReentrantLock(initialConfig.fair());
        evictionScheduleChanged = poolStateLock.newCondition();
        shutdownProgressChanged = poolStateLock.newCondition();
        idleEvictionThread = new Thread(this::removeExpired, "connection-pool-cleaner");
        idleEvictionThread.setDaemon(true);
        idleEvictionThread.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long waitBudgetNanos = nonNegativeNanos(timeout, "timeout");
        long waitStartedAtNanos = System.nanoTime();
        checkRunning();
        long lockWaitNanos = Math.max(0L, waitBudgetNanos - (System.nanoTime() - waitStartedAtNanos));
        if (!poolStateLock.tryLock(lockWaitNanos, TimeUnit.NANOSECONDS)) {
            checkRunning();
            throw new PoolTimeoutException(timeout);
        }
        try {
            checkRunning();
            if (waitBudgetNanos > 0 && System.nanoTime() - waitStartedAtNanos >= waitBudgetNanos) {
                throw new PoolTimeoutException(timeout);
            }
            if (!pendingBorrows.isEmpty() || !available()) {
                Waiter borrowRequest = new Waiter(poolStateLock.newCondition());
                pendingBorrows.addLast(borrowRequest);
                try {
                    while (true) {
                        checkRunning();
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                        long remainingWaitNanos = waitBudgetNanos - (System.nanoTime() - waitStartedAtNanos);
                        if (remainingWaitNanos <= 0) {
                            throw new PoolTimeoutException(timeout);
                        }
                        if (pendingBorrows.peekFirst() == borrowRequest && available()) {
                            break;
                        }
                        borrowRequest.resourceReady().awaitNanos(remainingWaitNanos);
                    }
                } catch (InterruptedException | PoolTimeoutException | RuntimeException borrowFailure) {
                    pendingBorrows.remove(borrowRequest);
                    signalNext();
                    throw borrowFailure;
                }
                pendingBorrows.removeFirst();
            }
            if (!idleConnections.isEmpty()) {
                Lease issuedLease = new Lease(idleConnections.removeFirst().physicalConnection());
                activeLeases.add(issuedLease);
                evictionScheduleChanged.signal();
                signalNext();
                return issuedLease;
            }
            pendingCreations++;
            signalNext();
        } finally {
            poolStateLock.unlock();
        }
        return createConnection();
    }

    private PooledConnection createConnection() throws ConnectionCreationException {
        RawConnection physicalConnection;
        try {
            physicalConnection = Objects.requireNonNull(connectionFactory.create(), "factory result");
        } catch (Exception creationFailure) {
            poolStateLock.lock();
            try {
                pendingCreations--;
                signalNext();
            } finally {
                poolStateLock.unlock();
            }
            throw new ConnectionCreationException(creationFailure);
        }

        poolStateLock.lock();
        try {
            pendingCreations--;
            if (!shutdownStarted) {
                Lease issuedLease = new Lease(physicalConnection);
                activeLeases.add(issuedLease);
                return issuedLease;
            }
            pendingCloses++;
        } finally {
            poolStateLock.unlock();
        }
        closeDetached(physicalConnection);
        throw new IllegalStateException("Pool is shut down");
    }

    @Override
    public void resize(int requestedCapacity) {
        if (requestedCapacity < initialConfig.minIdle() || requestedCapacity < 1 || requestedCapacity > PoolConfig.MAX_POOL_SIZE) {
            throw new IllegalArgumentException("Invalid maxSize: " + requestedCapacity);
        }
        List<RawConnection> detachedConnections = new ArrayList<>();
        poolStateLock.lock();
        try {
            checkRunning();
            this.capacityLimit = requestedCapacity;
            while (total() > requestedCapacity && !idleConnections.isEmpty()) {
                detachFirstIdle(detachedConnections);
            }
            signalNext();
            evictionScheduleChanged.signal();
        } finally {
            poolStateLock.unlock();
        }
        closeDetached(detachedConnections);
    }

    @Override
    public PoolConfig config() {
        poolStateLock.lock();
        try {
            return initialConfig.withMaxSize(capacityLimit);
        } finally {
            poolStateLock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        poolStateLock.lock();
        try {
            return new PoolStats(total(), idleConnections.size(), activeLeases.size(), pendingCreations, pendingBorrows.size());
        } finally {
            poolStateLock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long returnGraceNanos = nonNegativeNanos(grace, "grace");
        long shutdownStartedAtNanos = System.nanoTime();
        List<RawConnection> detachedConnections = new ArrayList<>();
        poolStateLock.lock();
        try {
            if (shutdownStarted) {
                return;
            }
            shutdownStarted = true;
            for (Waiter borrowRequest : pendingBorrows) {
                borrowRequest.resourceReady().signal();
            }
            evictionScheduleChanged.signal();
            while (!idleConnections.isEmpty()) {
                detachFirstIdle(detachedConnections);
            }
        } finally {
            poolStateLock.unlock();
        }
        boolean shutdownInterrupted = Thread.interrupted();
        closeDetached(detachedConnections);
        detachedConnections.clear();

        poolStateLock.lock();
        try {
            try {
                while (!activeLeases.isEmpty() && !shutdownInterrupted) {
                    long remainingGraceNanos = returnGraceNanos - (System.nanoTime() - shutdownStartedAtNanos);
                    if (remainingGraceNanos <= 0) {
                        break;
                    }
                    shutdownProgressChanged.awaitNanos(remainingGraceNanos);
                }
            } catch (InterruptedException interruption) {
                shutdownInterrupted = true;
            }
            for (Lease issuedLease : activeLeases) {
                issuedLease.invalidated = true;
                detachedConnections.add(issuedLease.physicalConnection);
                pendingCloses++;
            }
            activeLeases.clear();
        } finally {
            poolStateLock.unlock();
        }
        closeDetached(detachedConnections);

        while (true) {
            try {
                idleEvictionThread.join();
                break;
            } catch (InterruptedException interruption) {
                shutdownInterrupted = true;
            }
        }
        poolStateLock.lock();
        try {
            while (pendingCloses != 0) {
                try {
                    shutdownProgressChanged.await();
                } catch (InterruptedException interruption) {
                    shutdownInterrupted = true;
                }
            }
        } finally {
            poolStateLock.unlock();
        }
        shutdownInterrupted |= Thread.interrupted();
        if (shutdownInterrupted) {
            throw new InterruptedException();
        }
    }

    private void removeExpired() {
        while (true) {
            List<RawConnection> detachedConnections = new ArrayList<>();
            poolStateLock.lock();
            try {
                while (!shutdownStarted && detachedConnections.isEmpty()) {
                    if (idleConnections.size() <= initialConfig.minIdle()) {
                        evictionScheduleChanged.await();
                        continue;
                    }
                    long nanosUntilExpiry = idleTimeoutNanos - (System.nanoTime() - idleConnections.getFirst().idleSinceNanos());
                    if (nanosUntilExpiry > 0) {
                        evictionScheduleChanged.awaitNanos(nanosUntilExpiry);
                        continue;
                    }
                    long evictionCheckAtNanos = System.nanoTime();
                    while (idleConnections.size() > initialConfig.minIdle()
                            && evictionCheckAtNanos - idleConnections.getFirst().idleSinceNanos() >= idleTimeoutNanos) {
                        detachFirstIdle(detachedConnections);
                    }
                    signalNext();
                }
                if (shutdownStarted) {
                    return;
                }
            } catch (InterruptedException interruption) {
                continue;
            } finally {
                poolStateLock.unlock();
            }
            closeDetached(detachedConnections);
        }
    }

    private int total() {
        return idleConnections.size() + activeLeases.size() + pendingCreations;
    }

    private boolean available() {
        return !idleConnections.isEmpty() || total() < capacityLimit;
    }

    private void signalNext() {
        if (!shutdownStarted && !pendingBorrows.isEmpty() && available()) {
            pendingBorrows.getFirst().resourceReady().signal();
        }
    }

    private void checkRunning() {
        if (shutdownStarted) {
            throw new IllegalStateException("Pool is shut down");
        }
    }

    private void detachFirstIdle(List<RawConnection> detachedConnections) {
        detachedConnections.add(idleConnections.removeFirst().physicalConnection());
        pendingCloses++;
    }

    private void closeDetached(List<RawConnection> detachedConnections) {
        for (RawConnection physicalConnection : detachedConnections) {
            closeDetached(physicalConnection);
        }
    }

    private void closeDetached(RawConnection physicalConnection) {
        try {
            physicalConnection.close();
        } catch (Exception closeFailure) {
        } finally {
            poolStateLock.lock();
            try {
                pendingCloses--;
                shutdownProgressChanged.signalAll();
            } finally {
                poolStateLock.unlock();
            }
        }
    }

    private static long nonNegativeNanos(Duration requestedDuration, String argumentName) {
        Objects.requireNonNull(requestedDuration, argumentName);
        if (requestedDuration.isNegative()) {
            throw new IllegalArgumentException(argumentName + " must not be negative");
        }
        return toNanos(requestedDuration);
    }

    private static long toNanos(Duration requestedDuration) {
        try {
            return requestedDuration.toNanos();
        } catch (ArithmeticException durationOverflow) {
            return Long.MAX_VALUE;
        }
    }

    private record IdleConnection(RawConnection physicalConnection, long idleSinceNanos) {
    }

    private record Waiter(Condition resourceReady) {
    }

    private final class Lease implements PooledConnection {
        private final RawConnection physicalConnection;
        private boolean invalidated;

        private Lease(RawConnection physicalConnection) {
            this.physicalConnection = physicalConnection;
        }

        @Override
        public RawConnection raw() {
            poolStateLock.lock();
            try {
                if (invalidated) {
                    throw new IllegalStateException("Connection is returned or closed");
                }
                return physicalConnection;
            } finally {
                poolStateLock.unlock();
            }
        }

        @Override
        public void close() {
            boolean discardOnReturn;
            poolStateLock.lock();
            try {
                if (invalidated) {
                    return;
                }
                invalidated = true;
                discardOnReturn = shutdownStarted || total() > capacityLimit;
                activeLeases.remove(this);
                if (discardOnReturn) {
                    pendingCloses++;
                } else {
                    idleConnections.addLast(new IdleConnection(physicalConnection, System.nanoTime()));
                }
                signalNext();
                evictionScheduleChanged.signal();
                shutdownProgressChanged.signalAll();
            } finally {
                poolStateLock.unlock();
            }
            if (discardOnReturn) {
                closeDetached(physicalConnection);
            }
        }
    }
}
