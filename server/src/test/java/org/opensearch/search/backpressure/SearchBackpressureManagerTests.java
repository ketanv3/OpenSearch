/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.search.backpressure.TestHelpers.createMockTaskWithResourceStats;

public class SearchBackpressureManagerTests extends OpenSearchTestCase {

    public void testIsNodeInDuress() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);

        AtomicReference<Double> cpuUsage = new AtomicReference<>();
        AtomicReference<Double> heapUsage = new AtomicReference<>();
        DoubleSupplier cpuUsageSupplier = cpuUsage::get;
        DoubleSupplier heapUsageSupplier = heapUsage::get;

        SearchBackpressureManager manager = new SearchBackpressureManager(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            mockTaskResourceTrackingService,
            mockThreadPool,
            System::nanoTime,
            cpuUsageSupplier,
            heapUsageSupplier
        );

        // node not in duress
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(manager.isNodeInDuress());

        // node in duress; but not for many consecutive data points
        cpuUsage.set(1.0);
        heapUsage.set(1.0);
        assertFalse(manager.isNodeInDuress());

        // node in duress for consecutive data points
        assertFalse(manager.isNodeInDuress());
        assertTrue(manager.isNodeInDuress());

        // node not in duress anymore
        cpuUsage.set(0.0);
        heapUsage.set(0.0);
        assertFalse(manager.isNodeInDuress());
    }

    public void testGetTaskCancellations() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);

        doReturn(Map.of(
            1L, createMockTaskWithResourceStats(SearchShardTask.class, CpuUsageTracker.CPU_TIME_NANOS_THRESHOLD + 1, 0, mockTimeNanosSupplier.getAsLong()),
            2L, createMockTaskWithResourceStats(SearchShardTask.class, CpuUsageTracker.CPU_TIME_NANOS_THRESHOLD + 1, 0, mockTimeNanosSupplier.getAsLong() - ElapsedTimeTracker.ELAPSED_TIME_NANOS_THRESHOLD),
            3L, createMockTaskWithResourceStats(SearchShardTask.class, 0, 0, mockTimeNanosSupplier.getAsLong()),
            4L, createMockTaskWithResourceStats(Task.class, 100, 200)  // generic task; not eligible for search backpressure
        )).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        SearchBackpressureManager manager = new SearchBackpressureManager(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 0,
            () -> 0
        );

        // there are three search shard tasks
        List<CancellableTask> searchShardTasks = manager.getSearchShardTasks();
        assertEquals(3, searchShardTasks.size());

        // but only two of them are breaching thresholds
        List<TaskCancellation> taskCancellations = manager.getTaskCancellations(searchShardTasks);
        assertEquals(2, taskCancellations.size());

        // task cancellations are sorted in reverse order of the score
        assertEquals(2, taskCancellations.get(0).getReasons().size());
        assertEquals(1, taskCancellations.get(1).getReasons().size());
        assertEquals(2, taskCancellations.get(0).totalCancellationScore());
        assertEquals(1, taskCancellations.get(1).totalCancellationScore());
    }
}
