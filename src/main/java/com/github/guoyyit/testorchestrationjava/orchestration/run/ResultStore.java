package com.github.guoyyit.testorchestrationjava.orchestration.run;

import com.github.guoyyit.testorchestrationjava.orchestration.model.AttemptRecord;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseResult;
import com.github.guoyyit.testorchestrationjava.orchestration.model.CaseStatus;
import com.github.guoyyit.testorchestrationjava.orchestration.model.SkipReason;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrency-safe, all-or-nothing result sink.
 *
 * <p>Attempts are appended before the final status is published; a final status
 * is committed exactly once (compare-and-set under the entry lock). A committed
 * {@link CaseResult} always carries the full immutable attempt list, so there is
 * never a visible final status with partial/missing attempts, and an unfinished
 * case can never appear as PASSED. Appended attempts of an unfinished case are
 * retained (visible via {@link #attemptsOf}) for crash traceability.</p>
 */
public final class ResultStore {

    private static final class Entry {
        final List<AttemptRecord> attempts = new ArrayList<>();
        volatile CaseResult finalized;
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    public ResultStore() {
    }

    public void register(Iterable<String> caseIds) {
        caseIds.forEach(id -> entries.computeIfAbsent(id, k -> new Entry()));
    }

    /** Appends an attempt record for a case, in invocation order. */
    public void appendAttempt(String caseId, AttemptRecord attempt) {
        Entry entry = entries.computeIfAbsent(caseId, k -> new Entry());
        synchronized (entry) {
            if (entry.finalized != null) {
                throw new IllegalStateException(
                        "attempt appended after finalization for case: " + caseId);
            }
            entry.attempts.add(attempt);
        }
    }

    /**
     * Publishes the unique final status atomically with the attempt list.
     *
     * @return false if the case was already finalized (the prior result stands)
     */
    public boolean finalizeCase(String caseId, CaseStatus status, SkipReason reason,
                                String plannedUnitId, String finalUnitId,
                                boolean flaky, boolean quarantined) {
        Entry entry = entries.computeIfAbsent(caseId, k -> new Entry());
        synchronized (entry) {
            if (entry.finalized != null) {
                return false;
            }
            List<AttemptRecord> snapshot = List.copyOf(entry.attempts);
            entry.finalized = new CaseResult(caseId, status, reason, snapshot,
                    plannedUnitId, finalUnitId, flaky, quarantined);
            return true;
        }
    }

    public boolean isFinalized(String caseId) {
        Entry entry = entries.get(caseId);
        return entry != null && entry.finalized != null;
    }

    public Optional<CaseStatus> statusOf(String caseId) {
        Entry entry = entries.get(caseId);
        return entry == null || entry.finalized == null
                ? Optional.empty()
                : Optional.of(entry.finalized.status());
    }

    /** Snapshot of attempts so far (or the finalized immutable list). */
    public List<AttemptRecord> attemptsOf(String caseId) {
        Entry entry = entries.get(caseId);
        if (entry == null) {
            return List.of();
        }
        synchronized (entry) {
            return List.copyOf(entry.attempts);
        }
    }

    /** Immutable, deterministic (case-id sorted) snapshot of finalized results. */
    public List<CaseResult> snapshot() {
        List<CaseResult> out = new ArrayList<>();
        for (Entry entry : entries.values()) {
            CaseResult r = entry.finalized;
            if (r != null) {
                out.add(r);
            }
        }
        out.sort(Comparator.comparing(CaseResult::caseId));
        return List.copyOf(out);
    }
}
