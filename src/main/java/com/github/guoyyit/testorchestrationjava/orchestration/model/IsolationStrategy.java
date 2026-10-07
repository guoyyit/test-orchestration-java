package com.github.guoyyit.testorchestrationjava.orchestration.model;

/** How cases identified as unstable are isolated from the stable lane. */
public enum IsolationStrategy {
    /** No historical isolation; unstable cases still retry in the stable lane. */
    NONE,
    /** Known unstable cases run after stable cases, in the quarantine lane. */
    QUARANTINE_AFTER,
    /** Known unstable cases are skipped before execution starts. */
    SKIP_QUARANTINED
}
