/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;

import java.util.List;
import java.util.stream.Collectors;

public class TaskCancellation implements Comparable<TaskCancellation> {
    private final CancellableTask task;

    private final List<Reason> reasons;

    public TaskCancellation(CancellableTask task, List<Reason> reasons) {
        this.task = task;
        this.reasons = reasons;
    }

    public void cancel() {
        String message = reasons.stream().map(Reason::getMessage).collect(Collectors.joining(","));
        task.cancel("resource consumption exceeded [" + message + "]");
        reasons.forEach(reason -> reason.getTracker().incrementCancellations());
    }

    public CancellableTask getTask() {
        return task;
    }

    public List<Reason> getReasons() {
        return reasons;
    }

    public int cancellationScore() {
        return reasons.stream().mapToInt(Reason::getCancellationScore).sum();
    }

    public boolean isEligibleForCancellation() {
        return (task.isCancelled() == false) && (reasons.size() > 0);
    }

    @Override
    public int compareTo(TaskCancellation other) {
        return Integer.compare(cancellationScore(), other.cancellationScore());
    }

    public static class Reason {
        private final ResourceUsageTracker tracker;
        private final String message;
        private final int cancellationScore;

        public Reason(ResourceUsageTracker tracker, String message, int cancellationScore) {
            this.tracker = tracker;
            this.message = message;
            this.cancellationScore = cancellationScore;
        }

        public ResourceUsageTracker getTracker() {
            return tracker;
        }

        public String getMessage() {
            return message;
        }

        public int getCancellationScore() {
            return cancellationScore;
        }
    }
}
