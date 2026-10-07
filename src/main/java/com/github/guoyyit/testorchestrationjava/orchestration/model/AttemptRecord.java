package com.github.guoyyit.testorchestrationjava.orchestration.model;

/**
 * Immutable record of one attempt at executing a case.
 *
 * @param attemptNumber 1-based attempt number
 * @param assignedUnitId unit the case was planned onto
 * @param executedUnitId unit that actually ran the attempt (may differ after a crash re-dispatch)
 * @param status attempt outcome
 * @param failureType classification when status is a failure, otherwise null
 * @param failureMessage message of the failure, or null
 * @param durationMillis wall-clock duration of the attempt
 * @param interrupted true if the attempt was interrupted by timeout/cancellation
 * @param crashed true if the executing unit crashed during the attempt
 */
public record AttemptRecord(
        int attemptNumber,
        String assignedUnitId,
        String executedUnitId,
        AttemptStatus status,
        FailureType failureType,
        String failureMessage,
        long durationMillis,
        boolean interrupted,
        boolean crashed
) {
}
