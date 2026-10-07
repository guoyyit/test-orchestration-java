package com.github.guoyyit.testorchestrationjava.orchestration.model;

/** Why a case was skipped rather than passed/failed. */
public enum SkipReason {
    /** A declared dependency did not pass. */
    DEPENDENCY_FAILED,
    /** The run was cancelled before the case completed. */
    CANCELLED,
    /** The run-level timeout elapsed before the case completed. */
    RUN_TIMEOUT,
    /** Its execution unit crashed and no surviving unit could take it over. */
    UNIT_CRASHED,
    /** Held out by the SKIP_QUARANTINED isolation strategy. */
    QUARANTINED
}
