/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.search.backpressure.Thresholds;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MemoryUsageTracker extends ResourceUsageTracker {
    private static final Logger logger = LogManager.getLogger(MemoryUsageTracker.class);

    private final MovingAverage movingAverage = new MovingAverage(100);

    @Override
    public String name() {
        return "memory_usage_tracker";
    }

    @Override
    public void update(Task task) {
        movingAverage.record(task.getTotalResourceStats().getMemoryInBytes());
    }

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        // There haven't been enough measurements.
        if (movingAverage.isReady() == false) {
            return Optional.empty();
        }

        double taskHeapUsage = task.getTotalResourceStats().getMemoryInBytes();
        double averageHeapUsage = movingAverage.getAverage();
        double allowedHeapUsage = averageHeapUsage * Thresholds.SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD;

        if (taskHeapUsage < Thresholds.SEARCH_TASK_HEAP_USAGE_THRESHOLD_BYTES || taskHeapUsage < allowedHeapUsage) {
            return Optional.empty();
        }

        return Optional.of(new TaskCancellation.Reason(this, "memory usage exceeded", (int) (taskHeapUsage / averageHeapUsage)));
    }

    @Override
    public Map<String, Double> currentStats(List<Task> activeTasks) {
        double currentMax = activeTasks.stream().mapToDouble(t -> t.getTotalResourceStats().getMemoryInBytes()).max().orElse(0);
        double currentAvg = activeTasks.stream().mapToDouble(t -> t.getTotalResourceStats().getMemoryInBytes()).average().orElse(0);

        return new MapBuilder<String, Double>()
            .put("current_max", currentMax)
            .put("current_avg", currentAvg)
            .put("rolling_avg", movingAverage.getAverage())
            .immutableMap();
    }
}
