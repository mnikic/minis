package com.minis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Benchmark {

  public static final int THREAD_COUNT = 1;
  public static final int ITERATIONS_PER_THREAD = 10000;

  public static void runJavaBench() throws InterruptedException {
    System.out.println("\n[Java ConcurrentHashMap] Warming up...");
    final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    long start = System.nanoTime();
    runWorkload((key, val) -> {
      map.put(key, val);
      map.get(key);
      map.remove(key);
    });
    long duration = System.nanoTime() - start;

    printStats("Java ConcurrentHashMap", duration);
  }

  public static void runMinisBench(Minis minis) throws InterruptedException {
    System.out.println("\n[Minis JNI] Warming up...");

    long start = System.nanoTime();
    runWorkload((key, val) -> {
      minis.set(key, val);
      minis.get(key);
      minis.del(key);
    });
    long duration = System.nanoTime() - start;

    printStats("Minis JNI", duration);
  }

  public static void printStats(String name, long durationNs) {
    double seconds = durationNs / 1_000_000_000.0;
    long totalOps = (long) THREAD_COUNT * ITERATIONS_PER_THREAD * 3; // Set + Get + Del
    long opsPerSec = (long) (totalOps / seconds);

    System.out.println("------------------------------------------------");
    System.out.printf("Results for %s:%n", name);
    System.out.printf("  Time: %.3fs%n", seconds);
    System.out.printf("  Throughput: %,d ops/sec%n", opsPerSec);
    System.out.println("------------------------------------------------");
  }

  // Functional interface to abstract the storage engine
  interface StorageAction {
    void apply(String key, String val);
  }

  private static void runWorkload(StorageAction action) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

    for (int i = 0; i < THREAD_COUNT; i++) {
      final int threadId = i;
      executor.submit(() -> {
        String prefix = "key_" + threadId + "_";
        String val = "value_payload_" + threadId;
        try {
          for (int j = 0; j < ITERATIONS_PER_THREAD; j++) {
            action.apply(prefix + j, val);
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await();
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }
}
