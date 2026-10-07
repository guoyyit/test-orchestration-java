package com.github.guoyyit.testorchestrationjava.orchestration.model;

/**
 * Capacity limit for a named resource tag. Cases declaring the tag may only
 * run when a permit is available; otherwise they queue.
 */
public record ResourceQuota(String tag, int capacity) {

    public ResourceQuota {
        if (capacity < 1) {
            throw new IllegalArgumentException("quota capacity must be >= 1");
        }
    }
}
