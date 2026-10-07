package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import com.github.guoyyit.testorchestrationjava.orchestration.resource.ResourceLease;
import com.github.guoyyit.testorchestrationjava.orchestration.resource.ResourceManager;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourceManagerTest {

    @Test
    void globalConcurrencyIsNeverExceeded() throws Exception {
        ResourceManager rm = new ResourceManager(2, Map.of());
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        int tasks = 12;
        CountDownLatch done = new CountDownLatch(tasks);
        ExecutorService pool = Executors.newFixedThreadPool(6);
        TestCase tc = new TestCase("c", 1);
        for (int i = 0; i < tasks; i++) {
            pool.submit(() -> {
                try (ResourceLease lease = rm.acquire(tc)) {
                    int now = active.incrementAndGet();
                    peak.updateAndGet(p -> Math.max(p, now));
                    Thread.sleep(30);
                    active.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(0, rm.inFlight());
        assertTrue(peak.get() <= 2, "peak concurrency over cap: " + peak.get());
        pool.shutdownNow();
    }

    @Test
    void perLabelQuotaIsEnforced() throws Exception {
        ResourceManager rm = new ResourceManager(6, Map.of("db", 1));
        AtomicInteger dbActive = new AtomicInteger();
        AtomicInteger dbPeak = new AtomicInteger();
        int tasks = 8;
        CountDownLatch done = new CountDownLatch(tasks);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        TestCase dbCase = new TestCase("c", 1, Set.of("db"), Set.of(), List.of());
        for (int i = 0; i < tasks; i++) {
            pool.submit(() -> {
                try (ResourceLease lease = rm.acquire(dbCase)) {
                    int now = dbActive.incrementAndGet();
                    dbPeak.updateAndGet(p -> Math.max(p, now));
                    Thread.sleep(20);
                    dbActive.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(1, dbPeak.get(), "db quota must serialize");
        pool.shutdownNow();
    }

    @Test
    void mutexGroupActsAsQuotaOne() throws Exception {
        ResourceManager rm = new ResourceManager(8, Map.of());
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Set<String> overlaps = ConcurrentHashMap.newKeySet();
        int tasks = 6;
        CountDownLatch done = new CountDownLatch(tasks);
        ExecutorService pool = Executors.newFixedThreadPool(6);
        TestCase tc = new TestCase("c", 1, Set.of(), Set.of("serial"), List.of());
        for (int i = 0; i < tasks; i++) {
            final int idx = i;
            pool.submit(() -> {
                try (ResourceLease lease = rm.acquire(tc)) {
                    int now = active.incrementAndGet();
                    peak.updateAndGet(p -> Math.max(p, now));
                    overlaps.add("task-" + idx);
                    Thread.sleep(25);
                    active.decrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(1, peak.get(), "mutex group must never run concurrently");
        pool.shutdownNow();
    }

    @Test
    void cancelledWaitIsInterruptedAndReleasesNothing() throws Exception {
        ResourceManager rm = new ResourceManager(1, Map.of());
        TestCase tc = new TestCase("c", 1);
        ResourceLease first = rm.acquire(tc);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                rm.acquire(tc, () -> true);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });
        waiter.start();
        waiter.join(2000);
        assertTrue(interrupted.get());
        first.close();
        // subsequent acquisition must still succeed -> no permit leaked
        ResourceLease again = rm.acquire(tc);
        again.close();
    }
}
