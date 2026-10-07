package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CancelTest {

    @Test
    void externalCancelSkipsAllUnfinishedAndKeepsCompleted() throws Exception {
        List<TestCase> cases = List.of(
                tc("head", 1),
                tc("long", 1),
                tc("tail", 1));

        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(1).maxConcurrency(1).defaultTimeoutMillis(0L)
                .seed(0L).build();
        TestOrchestrator orch = new TestOrchestrator(config);

        // "long" blocks until the test thread cancels the run
        Thread cancel = new Thread(() -> {
            try {
                Thread.sleep(200);
                orch.cancel();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        cancel.start();
        RunResult result = orch.run(cases,
                TestSupport.executor()
                        .pass("head")
                        .body("long", (v, c) -> {
                            try {
                                Thread.sleep(60_000L);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw e;
                            }
                        })
                        .pass("tail")
                        .build());
        cancel.join();

        assertTrue(result.cancelled());
        assertEquals(Set.of("head"), result.passedIds());
        // long was in flight, tail never started: both skipped, neither failed/passed
        assertEquals(Set.of("long", "tail"), result.skippedIds());
        assertEquals(Set.of(), result.failedIds());
        assertEquals(SkipReason.CANCELLED, result.resultOf("long").skipReason());
        assertEquals(SkipReason.CANCELLED, result.resultOf("tail").skipReason());
        assertEquals(CaseStatus.SKIPPED, result.resultOf("long").status());
    }
}
