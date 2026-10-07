package com.github.guoyyit.testorchestrationjava.orchestration.model;

/** Classification of a failed attempt, used to decide whether retry is allowed. */
public enum FailureType {
    /** Assertion-like failure that must not be retried. */
    DETERMINISTIC,
    /** Environmental/flaky failure eligible for retry. */
    RETRIABLE
}
