package com.github.guoyyit.testorchestrationjava.orchestration.flaky;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Tracks unstable cases. A run contributes one "flaky observation" to a case
 * when that run saw at least one {@code RETRIABLE} failure for it (regardless
 * of whether retries ultimately passed). Once the accumulated observations
 * reach the configured threshold, the case is treated as known unstable and
 * the isolation strategy applies.
 *
 * <p>State is kept in memory and optionally persisted to a properties file
 * via atomic write (temp file in the same directory + ATOMIC_MOVE), so an
 * interrupted write never corrupts a previously persisted registry.</p>
 */
public final class FlakyRegistry {

    private final Path file;
    private final Map<String, Integer> counts = new TreeMap<>();

    public FlakyRegistry() {
        this.file = null;
    }

    private FlakyRegistry(Path file) {
        this.file = file;
    }

    /** Loads (or creates) a file-backed registry; a missing file starts empty. */
    public static FlakyRegistry fromFile(Path file) {
        FlakyRegistry registry = new FlakyRegistry(file);
        if (Files.exists(file)) {
            Properties props = new Properties();
            try (InputStream in = Files.newInputStream(file)) {
                props.load(in);
            } catch (IOException e) {
                throw new IllegalStateException("failed to read flaky registry: " + file, e);
            }
            for (String name : props.stringPropertyNames()) {
                try {
                    registry.counts.put(name, Integer.parseInt(props.getProperty(name)));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("corrupt flaky registry entry: " + name, e);
                }
            }
        }
        return registry;
    }

    /**
     * Records one run for a case.
     *
     * @param hadRetriableFailure true if any attempt failed with a retriable error
     * @param finalPassed         whether the case ultimately passed
     * @param flakyThreshold      observations at/above which the case is "known flaky"
     * @return true if this record pushes the case to (or keeps it at) known-flaky
     */
    public synchronized boolean recordRun(String caseId, boolean hadRetriableFailure,
                                          boolean finalPassed, int flakyThreshold) {
        if (hadRetriableFailure) {
            counts.merge(caseId, 1, Integer::sum);
        }
        return counts.getOrDefault(caseId, 0) >= Math.max(1, flakyThreshold);
    }

    public synchronized boolean isKnownFlaky(String caseId) {
        return isKnownFlaky(caseId, 1);
    }

    public synchronized boolean isKnownFlaky(String caseId, int flakyThreshold) {
        return counts.getOrDefault(caseId, 0) >= Math.max(1, flakyThreshold);
    }

    /** Immutable snapshot of known unstable case ids at the given threshold. */
    public synchronized Set<String> knownFlakyCases(int flakyThreshold) {
        Set<String> out = new TreeSet<>();
        int threshold = Math.max(1, flakyThreshold);
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            if (e.getValue() >= threshold) {
                out.add(e.getKey());
            }
        }
        return Collections.unmodifiableSet(out);
    }

    public synchronized int flakyCount(String caseId) {
        return counts.getOrDefault(caseId, 0);
    }

    public synchronized Map<String, Integer> snapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(counts));
    }

    /** Removes all observations (tests only). */
    public synchronized void clear() {
        counts.clear();
    }

    /** Atomically persists to the backing file. No-op for a memory-only registry. */
    public synchronized void persist() {
        if (file == null) {
            return;
        }
        Properties props = new Properties();
        counts.forEach((k, v) -> props.setProperty(k, Integer.toString(v)));
        Path parent = file.toAbsolutePath().getParent();
        try {
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = Files.createTempFile(parent, ".flaky-", ".tmp");
            try (OutputStream out = Files.newOutputStream(tmp)) {
                props.store(out, "flaky case registry; value = retriable-failure run count");
            }
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist flaky registry: " + file, e);
        }
    }
}
