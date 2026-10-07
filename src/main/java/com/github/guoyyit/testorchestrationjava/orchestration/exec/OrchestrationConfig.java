package com.github.guoyyit.testorchestrationjava.orchestration.exec;

import com.github.guoyyit.testorchestrationjava.orchestration.model.ResourceQuota;

import java.time.Duration;
import java.util.List;

/**
 * Tuning knobs for one orchestrated run.
 */
public record OrchestrationConfig(
        int concurrency,
        List<ResourceQuota> quotas,
        RetryPolicy retryPolicy,
        Duration caseTimeout,
        long shardSeed) {

    public OrchestrationConfig {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1");
        }
        quotas = quotas == null ? List.of() : List.copyOf(quotas);
        retryPolicy = retryPolicy == null ? RetryPolicy.noRetry() : retryPolicy;
        caseTimeout = caseTimeout == null ? Duration.ofMinutes(5) : caseTimeout;
    }

    public static OrchestrationConfig defaults() {
        return new OrchestrationConfig(1, List.of(), RetryPolicy.noRetry(), Duration.ofMinutes(5), 0L);
    }
}
