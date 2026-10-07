package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.executor.ExecutionContext;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestCaseView;
import com.github.guoyyit.testorchestrationjava.orchestration.executor.TestExecutable;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.mutex;
import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MutexConcurrencyTest {

    @Test
    void casesSharingMutexGroupNeverOverlap() throws Exception {
        AtomicInteger inGroup = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Object gate = new Object();
        TestExecutable guarded = (TestCaseView v, ExecutionContext ctx) -> {
            int now = inGroup.incrementAndGet();
            peak.updateAndGet(p -> Math.max(p, now));
            synchronized (gate) {
                gate.wait(60);
            }
            inGroup.decrementAndGet();
        };

        List<TestCase> cases = List.of(
                mutex("m0", 1, "serial"),
                mutex("m1", 1, "serial"),
                mutex("m2", 1, "serial"),
                mutex("m3", 1, "serial"));

        // release the wait shortly after start so they all attempt to run concurrently
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(200);
                synchronized (gate) {
                    gate.notifyAll();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        releaser.start();

        TestOrchestrator orch = new TestOrchestrator(
                OrchestrationConfig.builder().unitCount(4).maxConcurrency(4).seed(2L).build());
        RunResult result = orch.run(cases, cs -> {
            java.util.Map<String, TestExecutable> m = new java.util.HashMap<>();
            cases.forEach(c -> m.put(c.id(), guarded));
            return m;
        });

        assertEquals(Set.of("m0", "m1", "m2", "m3"), result.passedIds());
        assertEquals(1, peak.get(), "mutex group overlapped, peak=" + peak.get());
        releaser.join();
    }

    @Test
    void unrelatedCasesStillRunConcurrently() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        TestExecutable body = (v, c) -> {
            int now = active.incrementAndGet();
            peak.updateAndGet(p -> Math.max(p, now));
            Thread.sleep(80);
            active.decrementAndGet();
        };

        List<TestCase> cases = List.of(tc("u0", 1), tc("u1", 1), tc("u2", 1), tc("u3", 1));
        TestOrchestrator orch = new TestOrchestrator(
                OrchestrationConfig.builder().unitCount(4).maxConcurrency(4).seed(2L).build());
        RunResult result = orch.run(cases, cs -> {
            java.util.Map<String, TestExecutable> m = new java.util.HashMap<>();
            cases.forEach(c -> m.put(c.id(), body));
            return m;
        });
        assertEquals(Set.of("u0", "u1", "u2", "u3"), result.passedIds());
        assertTrue(peak.get() >= 2, "expected concurrency among unrelated cases, peak=" + peak.get());
    }
}
