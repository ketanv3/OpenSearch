/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * ElapsedTimeTracker evaluates if the task has been running for more time than allowed.
 */
public class ElapsedTimeTracker extends ResourceUsageTracker {
    public static final String NAME = "elapsed_time_tracker";
    public static final float ELAPSED_TIME_MILLIS_THRESHOLD = 30 * 1000;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void update(Task task) {
        // nothing to do
    }

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        if (System.currentTimeMillis() - task.getStartTime() < ELAPSED_TIME_MILLIS_THRESHOLD) {
            return Optional.empty();
        }

        return Optional.of(new TaskCancellation.Reason(this, "elapsed time exceeded", 1));
    }

    @Override
    public ResourceUsageTracker.Stats currentStats(List<Task> activeTasks) {
        long now = System.nanoTime();
        long currentMax = activeTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).max().orElse(0);
        long currentAvg = (long) activeTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).average().orElse(0);
        return new Stats(currentMax, currentAvg);
    }

    public static class Stats implements ResourceUsageTracker.Stats {
        private final long currentMax;
        private final long currentAvg;

        public Stats(long currentMax, long currentAvg) {
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder
                .startObject()
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentMax, currentAvg);
        }
    }
}
