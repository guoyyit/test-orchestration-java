package com.github.guoyyit.testorchestrationjava.orchestration.exec;

/**
 * Retry and quarantine configuration for unstable (flaky) cases.
 *
 * @param maxAttempts      total attempts allowed for a flaky case (1 = no retry)
 * @param quarantineOnPass when a flaky case only passes after retry, mark the
 *                         run summary so the case is traceable as quarantined
 */
public record RetryPolicy(int maxAttempts, boolean quarantineOnPass) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, false);
    }
}
