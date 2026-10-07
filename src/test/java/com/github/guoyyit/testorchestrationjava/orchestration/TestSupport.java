package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.executor.ExecutionContext;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestCaseView;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutable;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutor;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestFailureException;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.UnitCrashException;
import com.github.guoyyit.testorchestrationjava.orchestration.model.FailureType;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Small builder for test executors. */
final class TestSupport {

    private TestSupport() {
    }

    static ExecutorBuilder executor() {
        return new ExecutorBuilder();
    }

    static TestCase tc(String id, long duration, String... deps) {
        return new TestCase(id, duration, java.util.Set.of(), java.util.Set.of(), List.of(deps));
    }

    static TestCase mutex(String id, long duration, String group, String... deps) {
        return new TestCase(id, duration, java.util.Set.of(), java.util.Set.of(group), List.of(deps));
    }

    static TestCase label(String id, long duration, String label, String... deps) {
        return new TestCase(id, duration, java.util.Set.of(label), java.util.Set.of(), List.of(deps));
    }

    static final class ExecutorBuilder {
        private final Map<String, TestExecutable> bodies = new HashMap<>();
        private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

        ExecutorBuilder pass(String id) {
            bodies.put(id, (v, c) -> {
            });
            return this;
        }

        ExecutorBuilder fail(String id) {
            bodies.put(id, (v, c) -> {
                throw new AssertionError("deterministic failure of " + id);
            });
            return this;
        }

        ExecutorBuilder failRetriable(String id) {
            bodies.put(id, (v, c) -> {
                throw new TestFailureException(FailureType.RETRIABLE, "flaky env failure of " + id);
            });
            return this;
        }

        ExecutorBuilder flaky(String id, int failuresBeforePass) {
            AtomicInteger counter = new AtomicInteger();
            counters.put(id, counter);
            bodies.put(id, (v, c) -> {
                if (counter.getAndIncrement() < failuresBeforePass) {
                    throw new TestFailureException(FailureType.RETRIABLE,
                            "transient failure attempt " + c.attemptNumber());
                }
            });
            return this;
        }

        ExecutorBuilder crashOnce(String id) {
            AtomicInteger counter = new AtomicInteger();
            counters.put(id, counter);
            bodies.put(id, (v, c) -> {
                if (counter.getAndIncrement() == 0) {
                    throw new UnitCrashException("simulated crash on first attempt of " + id);
                }
            });
            return this;
        }

        ExecutorBuilder alwaysCrash(String id) {
            bodies.put(id, (v, c) -> {
                throw new UnitCrashException("simulated crash of " + id);
            });
            return this;
        }

        ExecutorBuilder sleep(String id, long millis) {
            bodies.put(id, (v, c) -> {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            });
            return this;
        }

        ExecutorBuilder body(String id, TestExecutable executable) {
            bodies.put(id, executable);
            return this;
        }

        AtomicInteger counter(String id) {
            return counters.get(id);
        }

        TestExecutor build() {
            Map<String, TestExecutable> snapshot = Map.copyOf(bodies);
            return cases -> {
                // cases without an explicit body pass by default
                Map<String, TestExecutable> resolved = new HashMap<>(snapshot);
                for (TestCase c : cases) {
                    resolved.putIfAbsent(c.id(), (v, ctx) -> {
                    });
                }
                return resolved;
            };
        }
    }
}
