package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class PoolStarvationTest {
    @Test
    void nestedBorrowsStallWhenEachWorkerAlreadyHoldsOneConnection() throws Exception {
        int workerCount = 4;
        AtomicInteger connectionIdSequence = new AtomicInteger();
        Map<String, Integer> heldConnectionIds = new ConcurrentHashMap<>();
        ConnectionPool connectionPool = new ConnectionPoolImpl(
                new PoolConfig(0, workerCount, Duration.ofMinutes(5), true),
                () -> new TestConnection(connectionIdSequence.incrementAndGet()));
        CountDownLatch initialBorrowsCompleted = new CountDownLatch(workerCount);
        CountDownLatch allowNestedBorrow = new CountDownLatch(1);
        AtomicBoolean teardownStarted = new AtomicBoolean();
        List<Thread> workerThreads = new ArrayList<>();
        List<FutureTask<Void>> workerTasks = new ArrayList<>();
        try {
            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                FutureTask<Void> workerTask = new FutureTask<>(() -> {
                    try (PooledConnection initialLease = connectionPool.borrow(Duration.ofSeconds(5))) {
                        TestConnection physicalConnection = (TestConnection) initialLease.raw();
                        heldConnectionIds.put(Thread.currentThread().getName(), physicalConnection.connectionId);
                        initialBorrowsCompleted.countDown();
                        allowNestedBorrow.await();
                        try (PooledConnection nestedLease = connectionPool.borrow(Duration.ofMinutes(1))) {
                            assertTrue(teardownStarted.get(), "No second connection exists before cancellation");
                        } catch (InterruptedException | IllegalStateException teardownException) {
                            assertTrue(teardownStarted.get());
                        }
                    }
                    return null;
                });
                Thread workerThread = new Thread(workerTask, "starvation-worker-" + (workerIndex + 1));
                workerTasks.add(workerTask);
                workerThreads.add(workerThread);
                workerThread.start();
            }
            assertTrue(initialBorrowsCompleted.await(5, TimeUnit.SECONDS));
            allowNestedBorrow.countDown();
            awaitWaiting(connectionPool, workerCount);
            assertEquals(new PoolStats(workerCount, 0, workerCount, 0, workerCount), connectionPool.stats());
            assertTrue(workerTasks.stream().noneMatch(FutureTask::isDone));

            if ("1".equals(System.getenv("POOL_CAPTURE_THREAD_DUMP"))) {
                captureThreadDump(workerCount, connectionPool.stats(), heldConnectionIds);
            }
        } finally {
            teardownStarted.set(true);
            allowNestedBorrow.countDown();
            connectionPool.shutdown(Duration.ZERO);
            for (Thread workerThread : workerThreads) {
                workerThread.interrupt();
                workerThread.join(2_000);
                assertFalse(workerThread.isAlive(), workerThread.getName());
            }
        }
        for (FutureTask<Void> workerTask : workerTasks) {
            workerTask.get(1, TimeUnit.SECONDS);
        }
        assertEquals(new PoolStats(0, 0, 0, 0, 0), connectionPool.stats());
    }

    @Test
    void minimumCapacityLetsEveryWorkerFinish() throws Exception {
        int workerCount = 4;
        int connectionsPerWorker = 2;
        int requiredCapacity = workerCount * (connectionsPerWorker - 1) + 1;
        ConnectionPool connectionPool = new ConnectionPoolImpl(
                new PoolConfig(0, requiredCapacity, Duration.ofMinutes(5), true), () -> new TestConnection(0));
        CountDownLatch partialBorrowsCompleted = new CountDownLatch(workerCount);
        List<Thread> workerThreads = new ArrayList<>();
        List<FutureTask<Void>> workerTasks = new ArrayList<>();
        try {
            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                FutureTask<Void> workerTask = new FutureTask<>(() -> {
                    List<PooledConnection> heldLeases = new ArrayList<>();
                    try {
                        for (int connectionIndex = 1; connectionIndex < connectionsPerWorker; connectionIndex++) {
                            heldLeases.add(connectionPool.borrow(Duration.ofSeconds(5)));
                        }
                        partialBorrowsCompleted.countDown();
                        assertTrue(partialBorrowsCompleted.await(5, TimeUnit.SECONDS));
                        heldLeases.add(connectionPool.borrow(Duration.ofSeconds(5)));
                        assertEquals(connectionsPerWorker, heldLeases.size());
                        for (PooledConnection heldLease : heldLeases) {
                            assertDoesNotThrow(heldLease::raw);
                        }
                    } finally {
                        for (PooledConnection heldLease : heldLeases) {
                            heldLease.close();
                        }
                    }
                    return null;
                });
                Thread workerThread = new Thread(workerTask, "sufficient-capacity-worker-" + (workerIndex + 1));
                workerTasks.add(workerTask);
                workerThreads.add(workerThread);
                workerThread.start();
            }
            for (FutureTask<Void> workerTask : workerTasks) {
                workerTask.get(8, TimeUnit.SECONDS);
            }
            assertEquals(0, connectionPool.stats().borrowed());
            assertEquals(0, connectionPool.stats().waiting());
            assertTrue(connectionPool.stats().total() <= requiredCapacity);
        } finally {
            connectionPool.shutdown(Duration.ZERO);
            for (Thread workerThread : workerThreads) {
                workerThread.interrupt();
                workerThread.join(2_000);
                assertFalse(workerThread.isAlive(), workerThread.getName());
            }
        }
    }

    private static void awaitWaiting(ConnectionPool connectionPool, int workerCount) throws InterruptedException {
        long coordinationDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (connectionPool.stats().waiting() != workerCount) {
            assertTrue(System.nanoTime() < coordinationDeadlineNanos, "Workers did not enter nested borrow");
            Thread.sleep(1);
        }
    }

    private static void captureThreadDump(int workerCount, PoolStats snapshot, Map<String, Integer> heldConnectionIds) throws Exception {
        Path dumpDirectory = Path.of("build/reports/starvation");
        Files.createDirectories(dumpDirectory);
        String jpsOutput = runTool(dumpDirectory.resolve("jps-full.txt"), "jps", "-l");
        String testProcessId = Long.toString(ProcessHandle.current().pid());
        String testProcessEntry = jpsOutput.lines().filter(processEntry -> processEntry.startsWith(testProcessId + " "))
                .findFirst().orElseThrow(() -> new AssertionError("Test JVM is absent from jps -l"));
        String targetProcessId = testProcessEntry.split(" ", 2)[0];
        Files.writeString(dumpDirectory.resolve("jps.txt"), testProcessEntry + System.lineSeparator());
        String threadDump = runTool(dumpDirectory.resolve("thread-dump.txt"), "jcmd", targetProcessId, "Thread.print");
        for (int workerIndex = 1; workerIndex <= workerCount; workerIndex++) {
            assertTrue(threadDump.contains("\"starvation-worker-" + workerIndex + "\""));
        }
        assertTrue(threadDump.contains("ConnectionPoolImpl.borrow"));
        assertFalse(threadDump.contains("Found one Java-level deadlock"));
        String captureDetails = "Captured: " + OffsetDateTime.now(ZoneId.of("Europe/Moscow")) + "\n"
                + "Commands: jps -l; jcmd " + targetProcessId + " Thread.print\n"
                + "jps selection: " + testProcessEntry + "\n"
                + "T=" + workerCount + ", C=2, maxSize=" + workerCount + ", second borrow timeout=PT1M\n"
                + snapshot + "\n"
                + "No worker completed before cancellation.\n"
                + "Found one Java-level deadlock: absent.\n";
        for (var ownershipEntry : new TreeMap<>(heldConnectionIds).entrySet()) {
            captureDetails += ownershipEntry.getKey() + " holds connection-" + ownershipEntry.getValue() + "; waits for a second connection.\n";
        }
        Files.writeString(dumpDirectory.resolve("capture.txt"), captureDetails);
        System.out.print(captureDetails);
    }

    private static String runTool(Path outputFile, String toolName, String... toolArguments) throws Exception {
        List<String> commandLine = new ArrayList<>();
        commandLine.add(Path.of(System.getProperty("java.home"), "bin", toolName).toString());
        commandLine.addAll(List.of(toolArguments));
        Process toolProcess = new ProcessBuilder(commandLine).redirectErrorStream(true)
                .redirectOutput(outputFile.toFile()).start();
        if (!toolProcess.waitFor(10, TimeUnit.SECONDS)) {
            toolProcess.destroyForcibly();
            throw new AssertionError(toolName + " did not finish");
        }
        assertEquals(0, toolProcess.exitValue(), toolName + " failed; see " + outputFile);
        return Files.readString(outputFile);
    }

    private static final class TestConnection implements RawConnection {
        private final int connectionId;

        private TestConnection(int connectionId) {
            this.connectionId = connectionId;
        }

        @Override
        public void close() {
        }
    }
}
