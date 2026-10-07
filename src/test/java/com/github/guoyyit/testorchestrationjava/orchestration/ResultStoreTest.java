package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.RunSummary;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.TestStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.result.ResultStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultStoreTest {

    private static TestResult result(String id, TestStatus status) {
        return new TestResult(id, status, 1, "executor-1", List.of(), null);
    }

    @Test
    void duplicateFinalCommitIsRejected() {
        ResultStore store = new ResultStore();
        store.commit(result("a", TestStatus.PASSED));
        assertThrows(IllegalStateException.class,
                () -> store.commit(result("a", TestStatus.FAILED)),
                "final conclusion must be unique");
        assertEquals(TestStatus.PASSED, store.find("a").orElseThrow().status(),
                "first commit wins");
    }

    @Test
    void uncommittedCasesAreSkippedNeverPassed() {
        ResultStore store = new ResultStore();
        store.commit(result("done", TestStatus.PASSED));
        RunSummary summary = store.summarize("run-x", List.of("done", "missing"));
        assertEquals(Set.of("done"), summary.passed());
        assertEquals(Set.of("missing"), summary.skipped(),
                "a case without a committed result must be skipped, not passed");
    }

    @Test
    void summarySetsAreDisjointAndSorted() {
        ResultStore store = new ResultStore();
        store.commit(result("b", TestStatus.FAILED));
        store.commit(result("a", TestStatus.PASSED));
        store.commit(result("c", TestStatus.SKIPPED));
        RunSummary summary = store.summarize("run-y", List.of("c", "b", "a"));
        assertEquals(List.of("a"), List.copyOf(summary.passed()));
        assertEquals(List.of("b"), List.copyOf(summary.failed()));
        assertEquals(List.of("c"), List.copyOf(summary.skipped()));
        int total = summary.passed().size() + summary.failed().size() + summary.skipped().size();
        assertEquals(3, total);
        assertTrue(summary.results().get("b").executorId().equals("executor-1"),
                "executor attribution must survive into the summary");
    }
}
