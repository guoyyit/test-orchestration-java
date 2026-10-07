package com.github.guoyyit.testorchestrationjava.orchestration.resource;

import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Enforces the global concurrency cap and per-label quotas (mutex groups are
 * labels with quota 1). Acquisition is fair and deadlock-free: labels are
 * always taken in lexicographic order. Callers over quota queue on fair FIFO
 * semaphores and wait; waiting is interruptible and cancellation-aware, so a
 * cancelled run never deadlocks the workers.
 */
public final class ResourceManager {

    private static final String MUTEX_PREFIX = "mutex::";

    private final Semaphore global;
    private final TreeMap<String, Semaphore> labelSemaphores = new TreeMap<>();
    private final int defaultLabelQuota;
    private final Map<String, Integer> labelQuotas;
    private final AtomicInteger inFlight = new AtomicInteger();

    public ResourceManager(int globalConcurrency, Map<String, Integer> labelQuotas) {
        if (globalConcurrency <= 0) {
            throw new IllegalArgumentException("globalConcurrency must be positive: " + globalConcurrency);
        }
        this.global = new Semaphore(globalConcurrency, true);
        this.defaultLabelQuota = globalConcurrency;
        this.labelQuotas = labelQuotas == null ? Map.of() : Map.copyOf(labelQuotas);
        for (Map.Entry<String, Integer> e : this.labelQuotas.entrySet()) {
            if (e.getValue() <= 0) {
                throw new IllegalArgumentException("quota must be positive for label: " + e.getKey());
            }
            labelSemaphores.put(e.getKey(), new Semaphore(e.getValue(), true));
        }
    }

    /** Blocks until every required label plus a global permit is available. */
    public ResourceLease acquire(TestCase testCase) throws InterruptedException {
        return acquire(testCase, () -> false);
    }

    /**
     * Blocks until permits are available; the wait is aborted with
     * {@link InterruptedException} if the current thread is interrupted or
     * {@code isCancelled} returns true. Already-acquired permits are released,
     * so no quota is leaked.
     */
    public ResourceLease acquire(TestCase testCase, BooleanSupplier isCancelled) throws InterruptedException {
        List<String> ordered = orderedLabels(testCase);
        List<String> held = new ArrayList<>(ordered.size() + 1);
        try {
            acquireOne(global, isCancelled);
            held.add("<global>");
            for (String label : ordered) {
                acquireOne(semaphoreFor(label), isCancelled);
                held.add(label);
            }
            inFlight.incrementAndGet();
            return new Lease(held);
        } catch (InterruptedException e) {
            releaseAll(held);
            throw e;
        }
    }

    public int inFlight() {
        return inFlight.get();
    }

    private void acquireOne(Semaphore semaphore, BooleanSupplier isCancelled) throws InterruptedException {
        // Bounded wait loop lets cancellation wake queued waiters even before a permit frees.
        while (true) {
            if (semaphore.tryAcquire(1, 50, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                return;
            }
            if (isCancelled.getAsBoolean()) {
                throw new InterruptedException("acquisition cancelled");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private Semaphore semaphoreFor(String internalLabel) {
        synchronized (labelSemaphores) {
            return labelSemaphores.computeIfAbsent(internalLabel,
                    k -> new Semaphore(quotaFor(k), true));
        }
    }

    private int quotaFor(String internalLabel) {
        if (internalLabel.startsWith(MUTEX_PREFIX)) {
            return 1;
        }
        Integer q = labelQuotas.get(internalLabel);
        return q == null ? defaultLabelQuota : q;
    }

    private List<String> orderedLabels(TestCase testCase) {
        TreeMap<String, Boolean> sorted = new TreeMap<>();
        for (String label : testCase.resourceLabels()) {
            sorted.put(label, Boolean.TRUE);
        }
        for (String group : testCase.mutexGroups()) {
            sorted.put(MUTEX_PREFIX + group, Boolean.TRUE);
        }
        return new ArrayList<>(sorted.keySet());
    }

    private void releaseAll(List<String> held) {
        // reverse order release
        for (int i = held.size() - 1; i >= 0; i--) {
            String label = held.get(i);
            if ("<global>".equals(label)) {
                global.release();
            } else {
                Semaphore s = labelSemaphores.get(label);
                if (s != null) {
                    s.release();
                }
            }
        }
    }

    private final class Lease implements ResourceLease {
        private final List<String> held;
        private volatile boolean closed;

        Lease(List<String> held) {
            this.held = List.copyOf(held);
        }

        @Override
        public List<String> heldLabels() {
            return held;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            releaseAll(held);
            inFlight.decrementAndGet();
        }
    }
}
