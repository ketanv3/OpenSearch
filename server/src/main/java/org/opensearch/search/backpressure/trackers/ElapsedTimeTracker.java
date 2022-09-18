/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ElapsedTimeTracker extends ResourceUsageTracker {
    public static final float ELAPSED_TIME_MILLIS_THRESHOLD = 30 * 1000;

    @Override
    public String name() {
        return "elapsed_time_tracker";
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
    public Map<String, Double> currentStats(List<Task> activeTasks) {
        long now = System.nanoTime();
        double currentMax = activeTasks.stream().mapToDouble(t -> now - t.getStartTimeNanos()).max().orElse(0);
        double currentAvg = activeTasks.stream().mapToDouble(t -> now - t.getStartTimeNanos()).average().orElse(0);

        return new MapBuilder<String, Double>()
            .put("current_max", currentMax)
            .put("current_avg", currentAvg)
            .immutableMap();
    }
}
