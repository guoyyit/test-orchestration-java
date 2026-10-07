package com.github.guoyyit.testorchestrationjava.orchestration.executor;

/**
 * Thrown by a test body to simulate a hard crash of the owning execution unit:
 * the in-flight attempt is recorded as CRASHED and remaining work of the unit is
 * redistributed to surviving units.
 */
public class UnitCrashException extends RuntimeException {

    public UnitCrashException(String message) {
        super(message);
    }
}
