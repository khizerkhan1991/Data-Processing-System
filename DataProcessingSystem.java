package com.example.dataprocessing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Multi-threaded Data Processing System example.
 */
public class DataProcessingSystem {

    private static final Logger LOGGER = Logger.getLogger(DataProcessingSystem.class.getName());

    /** Simple task: process an integer payload. */
    public static class Task {
        private final int id;
        private final int payload;
        private final boolean poisonPill;

        public Task(int id, int payload) {
            this.id = id;
            this.payload = payload;
            this.poisonPill = false;
        }

        private Task(boolean poisonPill) {
            this.id = -1;
            this.payload = 0;
            this.poisonPill = poisonPill;
        }

        public static Task poisonPill() {
            return new Task(true);
        }

        public boolean isPoisonPill() {
            return poisonPill;
        }

        public int getId() {
            return id;
        }

        public int getPayload() {
            return payload;
        }
    }

    /** Result of processing. */
    public static class Result {
        private final int taskId;
        private final int input;
        private final int output;
        private final LocalDateTime processedAt;

        public Result(int taskId, int input, int output) {
            this.taskId = taskId;
            this.input = input;
            this.output = output;
            this.processedAt = LocalDateTime.now();
        }

        @Override
        public String toString() {
            return "Result{taskId=" + taskId +
                    ", input=" + input +
                    ", output=" + output +
                    ", processedAt=" + processedAt +
                    '}';
        }
    }

    /** Thread-safe task queue with explicit locking. */
    public static class TaskQueue {
        private final List<Task> queue = new ArrayList<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();

        public void addTask(Task task) {
            lock.lock();
            try {
                queue.add(task);
                notEmpty.signal(); // wake one waiting worker
            } finally {
                lock.unlock();
            }
        }

        public Task getTask() throws InterruptedException {
            lock.lock();
            try {
                while (queue.isEmpty()) {
                    notEmpty.await();
                }
                return queue.remove(0);
            } finally {
                lock.unlock();
            }
        }
    }

    /** Worker that pulls tasks from the shared queue. */
    public static class Worker implements Runnable {
        private final int workerId;
        private final TaskQueue queue;
        private final List<Result> results;

        public Worker(int workerId, TaskQueue queue, List<Result> results) {
            this.workerId = workerId;
            this.queue = queue;
            this.results = results;
        }

        @Override
        public void run() {
            LOGGER.info(() -> "Worker " + workerId + " started.");
            try {
                while (true) {
                    Task task = queue.getTask(); // may block

                    if (task.isPoisonPill()) {
                        LOGGER.info(() -> "Worker " + workerId + " received poison pill. Exiting.");
                        break;
                    }

                    try {
                        Result result = processTask(task);
                        synchronized (results) {
                            results.add(result);
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE,
                                "Worker " + workerId + " error while processing task " + task.getId(), e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Worker " + workerId + " interrupted.", e);
            }
            LOGGER.info(() -> "Worker " + workerId + " completed.");
        }

        private Result processTask(Task task) throws InterruptedException {
            // Simulate CPU work with sleep
            Thread.sleep(100); // 100 ms

            int input = task.getPayload();
            int output = input * input; // e.g., square the number

            LOGGER.info(() -> "Worker " + workerId + " processed task " + task.getId()
                    + " input=" + input + " output=" + output);

            return new Result(task.getId(), input, output);
        }
    }

    /** Persist results to a file with basic error handling. */
    public static void writeResultsToFile(List<Result> results, String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (Result r : results) {
                writer.write(r.toString());
                writer.newLine();
            }
            LOGGER.info("Results successfully written to file: " + fileName);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error writing results to file: " + fileName, e);
        }
    }

    public static void main(String[] args) {
        final int NUM_WORKERS = 4;
        final int NUM_TASKS = 20;

        TaskQueue taskQueue = new TaskQueue();
        List<Result> results = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);

        // Start workers
        for (int i = 0; i < NUM_WORKERS; i++) {
            executor.submit(new Worker(i, taskQueue, results));
        }

        // Submit tasks
        for (int i = 0; i < NUM_TASKS; i++) {
            taskQueue.addTask(new Task(i, i + 1)); // payload = i+1
        }

        // Add one poison pill per worker to ensure clean termination
        for (int i = 0; i < NUM_WORKERS; i++) {
            taskQueue.addTask(Task.poisonPill());
        }

        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Write results
        writeResultsToFile(results, "java_results.txt");

        LOGGER.info("Data Processing System (Java) finished.");
        // Also print to console for screenshot
        results.forEach(System.out::println);
    }
}
