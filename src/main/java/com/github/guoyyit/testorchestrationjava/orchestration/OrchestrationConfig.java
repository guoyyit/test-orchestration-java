package com.github.guoyyit.testorchestrationjava.orchestration;

import com.github.guoyyit.testorchestrationjava.orchestration.model.IsolationStrategy;
import java.util.Map;

/**
 * Immutable configuration for one orchestrated run.
 * Use {@link #builder()} to construct.
 */
public final class OrchestrationConfig {

    private final int unitCount;
    private final int maxConcurrency;
    private final int maxRetries;
    private final long defaultTimeoutMillis;
    private final long runTimeoutMillis;
    private final long seed;
    private final Map<String, Integer> resourceQuotas;
    private final IsolationStrategy isolationStrategy;
    private final int flakyThreshold;
    private final int flakyMaxRetries;
    private final int quarantineConcurrency;

    private OrchestrationConfig(Builder b) {
        this.unitCount = b.unitCount;
        this.maxConcurrency = b.maxConcurrency;
        this.maxRetries = b.maxRetries;
        this.defaultTimeoutMillis = b.defaultTimeoutMillis;
        this.runTimeoutMillis = b.runTimeoutMillis;
        this.seed = b.seed;
        this.resourceQuotas = Map.copyOf(b.resourceQuotas);
        this.isolationStrategy = b.isolationStrategy;
        this.flakyThreshold = b.flakyThreshold;
        this.flakyMaxRetries = b.flakyMaxRetries;
        this.quarantineConcurrency = b.quarantineConcurrency;
    }

    // TODO skeleton: getters
    public int unitCount() { return unitCount; }
    public int maxConcurrency() { return maxConcurrency; }
    public int maxRetries() { return maxRetries; }
    public long defaultTimeoutMillis() { return defaultTimeoutMillis; }
    public long runTimeoutMillis() { return runTimeoutMillis; }
    public long seed() { return seed; }
    public Map<String, Integer> resourceQuotas() { return resourceQuotas; }
    public IsolationStrategy isolationStrategy() { return isolationStrategy; }
    public int flakyThreshold() { return flakyThreshold; }
    public int flakyMaxRetries() { return flakyMaxRetries; }
    public int quarantineConcurrency() { return quarantineConcurrency; }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link OrchestrationConfig}. */
    public static final class Builder {
        private int unitCount = 1;
        private int maxConcurrency = 1;
        private int maxRetries = 0;
        private long defaultTimeoutMillis = 30_000L;
        private long runTimeoutMillis = 0L;
        private long seed = 0L;
        private Map<String, Integer> resourceQuotas = Map.of();
        private IsolationStrategy isolationStrategy = IsolationStrategy.NONE;
        private int flakyThreshold = 1;
        private int flakyMaxRetries = 2;
        private int quarantineConcurrency = 1;

        public Builder unitCount(int v) { this.unitCount = v; return this; }
        public Builder maxConcurrency(int v) { this.maxConcurrency = v; return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }
        public Builder defaultTimeoutMillis(long v) { this.defaultTimeoutMillis = v; return this; }
        /** 0 means no run-level timeout. */
        public Builder runTimeoutMillis(long v) { this.runTimeoutMillis = v; return this; }
        public Builder seed(long v) { this.seed = v; return this; }
        public Builder resourceQuotas(Map<String, Integer> v) { this.resourceQuotas = v == null ? Map.of() : v; return this; }
        public Builder isolationStrategy(IsolationStrategy v) { this.isolationStrategy = v; return this; }
        /** Number of retriable failures (at least 1) after which a case is recorded as unstable. */
        public Builder flakyThreshold(int v) { this.flakyThreshold = v; return this; }
        public Builder flakyMaxRetries(int v) { this.flakyMaxRetries = v; return this; }
        public Builder quarantineConcurrency(int v) { this.quarantineConcurrency = v; return this; }

        public OrchestrationConfig build() {
            if (unitCount <= 0) {
                throw new IllegalArgumentException("unitCount must be positive: " + unitCount);
            }
            if (maxConcurrency <= 0) {
                throw new IllegalArgumentException("maxConcurrency must be positive: " + maxConcurrency);
            }
            if (quarantineConcurrency <= 0 || quarantineConcurrency > maxConcurrency) {
                throw new IllegalArgumentException(
                        "quarantineConcurrency must be in [1, maxConcurrency]");
            }
            if (maxRetries < 0 || flakyMaxRetries < 0) {
                throw new IllegalArgumentException("retry counts must be >= 0");
            }
            if (flakyThreshold <= 0) {
                throw new IllegalArgumentException("flakyThreshold must be >= 1");
            }
            if (defaultTimeoutMillis < 0 || runTimeoutMillis < 0) {
                throw new IllegalArgumentException("timeouts must be >= 0 (0 = disabled)");
            }
            for (Map.Entry<String, Integer> e : resourceQuotas.entrySet()) {
                if (e.getValue() == null || e.getValue() <= 0) {
                    throw new IllegalArgumentException("quota must be positive for label: " + e.getKey());
                }
            }
            return new OrchestrationConfig(this);
        }
    }
}
