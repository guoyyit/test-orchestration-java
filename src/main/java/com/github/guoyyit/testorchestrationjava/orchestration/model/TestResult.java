package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.util.List;

/**
 * Final, unique conclusion of a test case plus its full attempt history.
 */
public record TestResult(
        String caseId,
        TestStatus status,
        int attempts,
        String executorId,
        List<AttemptRecord> history,
        String error) {

    public TestResult {
        history = history == null ? List.of() : List.copyOf(history);
    }
}
