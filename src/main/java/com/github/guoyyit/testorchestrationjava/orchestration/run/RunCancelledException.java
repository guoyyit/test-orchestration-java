package com.github.guoyyit.testorchestrationjava.orchestration.run;

/** Internal signal: the run was cancelled while an attempt was queued/running. */
public class RunCancelledException extends RuntimeException {
    public RunCancelledException() {
        super("run cancelled");
    }
}
