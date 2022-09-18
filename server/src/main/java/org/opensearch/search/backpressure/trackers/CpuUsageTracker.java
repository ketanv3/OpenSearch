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

public class CpuUsageTracker extends ResourceUsageTracker {
    public static final float CPU_TIME_NANOS_THRESHOLD = 15 * 1000 * 1000;

    @Override
    public String name() {
        return "cpu_usage_tracker";
    }

    @Override
    public void update(Task task) {
        // nothing to do
    }

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        if (task.getTotalResourceStats().getCpuTimeInNanos() < CPU_TIME_NANOS_THRESHOLD) {
            return Optional.empty();
        }

        return Optional.of(new TaskCancellation.Reason(this, "cpu usage exceeded", 1));
    }

    @Override
    public Map<String, Double> currentStats(List<Task> activeTasks) {
        double currentMax = activeTasks.stream().mapToDouble(t -> t.getTotalResourceStats().getCpuTimeInNanos()).max().orElse(0);
        double currentAvg = activeTasks.stream().mapToDouble(t -> t.getTotalResourceStats().getCpuTimeInNanos()).average().orElse(0);

        return new MapBuilder<String, Double>()
            .put("current_max", currentMax)
            .put("current_avg", currentAvg)
            .immutableMap();
    }
}
