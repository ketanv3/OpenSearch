/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.ResourceStatsType;
import org.opensearch.tasks.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.ThreadResourceInfo;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceUsageTrackerTestsHelper {

    public static Task createMockTaskWithResourceStats(long cpuUsage, long memoryUsage) {
        ThreadResourceInfo threadResourceInfo = new ThreadResourceInfo(
            1L,
            ResourceStatsType.WORKER_STATS,
            new ResourceUsageMetric(ResourceStats.CPU, 0),
            new ResourceUsageMetric(ResourceStats.MEMORY, 0)
        );

        threadResourceInfo.recordResourceUsageMetrics(new ResourceUsageMetric(ResourceStats.CPU, cpuUsage));
        threadResourceInfo.recordResourceUsageMetrics(new ResourceUsageMetric(ResourceStats.MEMORY, memoryUsage));

        ConcurrentHashMap<Long, List< ThreadResourceInfo >> resourceStats = new ConcurrentHashMap<>();
        resourceStats.put(1L, List.of(threadResourceInfo));

        return new Task(
            123L,
            "",
            "",
            "",
            null,
            0,
            0,
            Collections.emptyMap(),
            resourceStats,
            Collections.emptyList()
        );
    }
}
