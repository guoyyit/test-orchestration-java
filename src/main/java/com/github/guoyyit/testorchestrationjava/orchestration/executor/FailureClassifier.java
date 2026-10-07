package com.github.guoyyit.testorchestrationjava.orchestration.executor;

import com.github.guoyyit.testorchestrationjava.orchestration.model.FailureType;

/**
 * Classifies uncaught throwables from a test body.
 * Assertion errors are always deterministic; explicit {@link TestFailureException}
 * carries its own classification; every other throwable defaults to retriable,
 * because environmental errors (IO, timeout, connection reset) are the usual
 * source of instability.
 */
@FunctionalInterface
public interface FailureClassifier {

    FailureType classify(Throwable throwable);

    /** Default classification rule. */
    FailureClassifier DEFAULT = throwable -> {
        if (throwable instanceof AssertionError) {
            return FailureType.DETERMINISTIC;
        }
        if (throwable instanceof TestFailureException tfe) {
            return tfe.failureType();
        }
        return FailureType.RETRIABLE;
    };
}
