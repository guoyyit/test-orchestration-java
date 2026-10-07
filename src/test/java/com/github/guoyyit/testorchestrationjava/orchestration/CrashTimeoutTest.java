package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptRecord;
import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.RunResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestCase;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static com.github.guoyyit.testorchestrationjava.orchestration.TestSupport.tc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashTimeoutTest {

    @Test
    void crashedAttemptIsRecordedAndCaseReDispatchedToSurvivingUnit() {
        // Two units: case "crash" crashes its unit once, then passes on the survivor.
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1);
        TestCase c = tc("c", 1);

        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(2)
                .maxConcurrency(2)
                .maxRetries(2)
                .seed(0L)
                .build();
        TestOrchestrator orch = new TestOrchestrator(config);
        RunResult result = orch.run(List.of(a, b, c),
                TestSupport.executor()
                        .crashOnce("a")
                        .pass("b")
                        .pass("c")
                        .build());

        assertEquals(Set.of("a", "b", "c"), result.passedIds(), result.toString());
        CaseResult cr = result.resultOf("a");
        assertTrue(cr.attempts().stream().anyMatch(at -> at.status() == AttemptStatus.CRASHED),
                "expected a CRASHED attempt, got " + cr.attempts());
        AttemptRecord crashed = cr.attempts().stream()
                .filter(at -> at.status() == AttemptStatus.CRASHED).findFirst().orElseThrow();
        assertTrue(crashed.crashed());
        // decisive attempt executed somewhere; traceability to a unit exists
        assertTrue(cr.finalExecutedUnitId() != null);
    }

    @Test
    void crashAttemptHistoryIsNotLost() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1);
        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(2).maxConcurrency(2).maxRetries(3).seed(0L).build();
        TestOrchestrator orch = new TestOrchestrator(config);
        RunResult result = orch.run(List.of(a, b),
                TestSupport.executor().crashOnce("a").pass("b").build());
        // CRASHED then PASSED: both records retained, attempt numbers ordered
        CaseResult cr = result.resultOf("a");
        assertEquals(AttemptStatus.CRASHED, cr.attempts().get(0).status());
        assertEquals(AttemptStatus.PASSED, cr.attempts().get(1).status());
    }

    @Test
    void perCaseTimeoutIsRetriedThenFailsWhenBudgetExhausted() {
        TestCase slow = tc("slow", 1);
        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(1).maxConcurrency(1).maxRetries(1)
                .defaultTimeoutMillis(80L)
                .seed(0L)
                .build();
        TestOrchestrator orch = new TestOrchestrator(config);
        long start = System.currentTimeMillis();
        RunResult result = orch.run(List.of(slow),
                TestSupport.executor().sleep("slow", 5_000L).build());
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(Set.of("slow"), result.failedIds());
        CaseResult cr = result.resultOf("slow");
        assertEquals(2, cr.attempts().size());
        assertEquals(AttemptStatus.TIMEOUT, cr.attempts().get(0).status());
        assertEquals(AttemptStatus.TIMEOUT, cr.attempts().get(1).status());
        assertTrue(elapsed < 3_000L, "test must not wait for the sleeping body: " + elapsed);
    }

    @Test
    void runTimeoutSkipsUnfinishedCasesAndKeepsCompletedResults() {
        TestCase done = tc("done", 1);
        TestCase slow = tc("slow", 1);
        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(1).maxConcurrency(1)
                .defaultTimeoutMillis(0L)
                .runTimeoutMillis(150L)
                .seed(0L)
                .build();
        TestOrchestrator orch = new TestOrchestrator(config);
        RunResult result = orch.run(List.of(done, slow),
                TestSupport.executor().pass("done").sleep("slow", 5_000L).build());

        assertTrue(result.timedOut());
        assertEquals(Set.of("done"), result.passedIds(), "completed result must survive");
        assertEquals(Set.of("slow"), result.skippedIds(), "unfinished must be skipped");
        assertEquals(com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason.RUN_TIMEOUT,
                result.resultOf("slow").skipReason());
        assertEquals(Set.of(), result.failedIds());
    }

    @Test
    void plannedAndFinalUnitsAreTraceablePerCase() {
        TestCase a = tc("a", 1);
        TestCase b = tc("b", 1);
        OrchestrationConfig config = OrchestrationConfig.builder()
                .unitCount(2).maxConcurrency(2).seed(0L).build();
        TestOrchestrator orch = new TestOrchestrator(config);
        RunResult result = orch.run(List.of(a, b),
                TestSupport.executor().pass("a").pass("b").build());
        for (String id : List.of("a", "b")) {
            CaseResult cr = result.resultOf(id);
            assertTrue(cr.plannedUnitId() != null && cr.finalExecutedUnitId() != null);
            assertNotEquals(null, cr.attempts().get(0).executedUnitId());
        }
    }
}
