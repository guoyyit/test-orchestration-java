package com.github.guoyyit.testorchestrationjava.orchestration.model;

/** Outcome of a single execution attempt. */
public enum AttemptStatus {
    PASSED,
    FAILED,
    SKIPPED,
    TIMEOUT,
    INTERRUPTED,
    CRASHED
}
