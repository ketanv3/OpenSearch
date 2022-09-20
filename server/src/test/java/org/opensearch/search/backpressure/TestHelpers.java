/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceUsage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHelpers {

    public static <T extends Task> T createMockTaskWithResourceStats(Class<T> type, long cpuUsage, long heapUsage) {
        return createMockTaskWithResourceStats(type, cpuUsage, heapUsage, 0);
    }

    public static <T extends Task> T createMockTaskWithResourceStats(Class<T> type, long cpuUsage, long heapUsage, long startTimeNanos) {
        T task = mock(type);
        when(task.getTotalResourceStats()).thenReturn(new TaskResourceUsage(cpuUsage, heapUsage));
        when(task.getStartTimeNanos()).thenReturn(startTimeNanos);
        return task;
    }
}
