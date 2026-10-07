package com.github.guoyyit.testorchestrationjava.orchestration.model;

/**
 * Lifecycle status of a single test case within a run.
 */
public enum TestStatus {
    /** Not yet scheduled. */
    PENDING,
    /** Currently executing on an executor unit. */
    RUNNING,
    /** Finished successfully. */
    PASSED,
    /** Finished with a deterministic failure (retries exhausted or not retryable). */
    FAILED,
    /** Not executed because a dependency failed/was skipped, or the run was cancelled. */
    SKIPPED
}
