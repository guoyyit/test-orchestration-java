package com.github.guoyyit.testorchestrationjava.orchestration.executor;

import com.github.guoyyit.testorchestrationjava.orchestration.model.FailureType;

/**
 * Failure raised by a test body with an explicit classification.
 * An {@link AssertionError} is always treated as {@link FailureType#DETERMINISTIC}
 * even without this exception.
 */
public class TestFailureException extends RuntimeException {

    private final FailureType failureType;

    public TestFailureException(FailureType failureType, String message) {
        super(message);
        this.failureType = failureType;
    }

    public TestFailureException(FailureType failureType, String message, Throwable cause) {
        super(message, cause);
        this.failureType = failureType;
    }

    public FailureType failureType() {
        return failureType;
    }
}
