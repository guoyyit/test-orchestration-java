package com.github.guoyyit.testorchestrationjava.orchestration.model;

import java.time.Instant;

/**
 * Audit record of a single execution attempt of a test case.
 */
public record AttemptRecord(
        int attempt,
        String executorId,
        Instant startedAt,
        Instant finishedAt,
        TestStatus status,
        String error) {
}
