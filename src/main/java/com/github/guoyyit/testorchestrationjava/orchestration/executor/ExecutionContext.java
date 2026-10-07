package com.github.guoyyit.testorchestrationjava.orchestration.executor;

/** Per-attempt context handed to the executable test body. */
public interface ExecutionContext {

    /** 1-based attempt number for this case within the current run. */
    int attemptNumber();

    /** Id of the execution unit running the attempt. */
    String unitId();

    /** True if the run has been cancelled; long-running bodies should poll this. */
    boolean isCancelled();

    /**
     * Throw an {@link InterruptedException}-like cooperative cancellation.
     * Bodies may ignore this, in which case timeout interruption is best-effort.
     */
    void checkCancelled() throws InterruptedException;
}
