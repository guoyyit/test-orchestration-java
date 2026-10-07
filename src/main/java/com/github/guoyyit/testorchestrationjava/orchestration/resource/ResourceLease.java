package com.github.guoyyit.testorchestrationjava.orchestration.resource;

import java.util.List;

/** Acquired resource permits; must be closed after the attempt finishes. */
public interface ResourceLease extends AutoCloseable {

    /** Ordered labels (including mutex groups) held by this lease. */
    List<String> heldLabels();

    @Override
    void close();
}
