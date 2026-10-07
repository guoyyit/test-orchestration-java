package com.github.guoyyit.testorchestrationjava.orchestration.exec;

/**
 * One execution unit (worker). Wraps a slot of the shared executor service and
 * enforces per-case timeout / cancellation.
 */
public class ExecutorUnit {

    private final String id;

    public ExecutorUnit(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }
}
