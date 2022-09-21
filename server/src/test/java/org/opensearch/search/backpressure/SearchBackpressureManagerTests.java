/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.stats.CancellationStats;
import org.opensearch.search.backpressure.stats.CancelledTaskStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.backpressure.TestHelpers.createMockTaskWithResourceStats;

public class SearchBackpressureManagerTests extends OpenSearchTestCase {

    public void testIsNodeInDuress() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);

        AtomicReference<Double> cpuUsage = new AtomicReference<>();
        AtomicReference<Double> heapUsage = new AtomicReference<>();
        DoubleSupplier cpuUsageSupplier = cpuUsage::get;
        DoubleSupplier heapUsageSupplier = heapUsage::get;

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            System::nanoTime,
            cpuUsageSupplier,
            heapUsageSupplier,
            Collections.emptyList()
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
        long cpuTimeThreshold = 100;
        long elapsedTimeThreshold = 500;

        doReturn(Map.of(
            1L, createMockTaskWithResourceStats(SearchShardTask.class, cpuTimeThreshold + 1, 0, mockTimeNanosSupplier.getAsLong()),
            2L, createMockTaskWithResourceStats(SearchShardTask.class, cpuTimeThreshold + 1, 0, mockTimeNanosSupplier.getAsLong() - elapsedTimeThreshold),
            3L, createMockTaskWithResourceStats(SearchShardTask.class, 0, 0, mockTimeNanosSupplier.getAsLong()),
            4L, createMockTaskWithResourceStats(CancellableTask.class, 100, 200)  // generic task; not eligible for search backpressure
        )).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 0,
            () -> 0,
            List.of(
                new CpuUsageTracker(() -> cpuTimeThreshold),
                new ElapsedTimeTracker(mockTimeNanosSupplier, () -> elapsedTimeThreshold)
            )
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

    public void testTrackerStateUpdateOnTaskCompletion() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        LongSupplier mockTimeNanosSupplier = () -> TimeUnit.SECONDS.toNanos(1234);
        ResourceUsageTracker mockTracker = mock(ResourceUsageTracker.class);

        SearchBackpressureSettings settings = new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 0.5,
            () -> 0.5,
            List.of(mockTracker)
        );

        // Record task completions to update the tracker state. Tasks other than SearchShardTask are ignored.
        manager.onTaskCompleted(createMockTaskWithResourceStats(CancellableTask.class, 100, 200));
        for (int i = 0; i < 100; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, 200));
        }
        assertEquals(100, manager.getCurrentIterationCompletedTasks());
        verify(mockTracker, times(100)).update(any());
    }

    public void testInFlightCancellation() {
        TaskResourceTrackingService mockTaskResourceTrackingService = mock(TaskResourceTrackingService.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        AtomicLong mockTime = new AtomicLong(0);
        LongSupplier mockTimeNanosSupplier = mockTime::get;

        class MockStats implements ResourceUsageTracker.Stats {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().endObject();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                return o != null && getClass() == o.getClass();
            }

            @Override
            public int hashCode() {
                return 0;
            }
        }

        ResourceUsageTracker mockTracker = new ResourceUsageTracker() {
            @Override
            public String name() {
                return "mock_tracker";
            }

            @Override
            public void update(Task task) {
            }

            @Override
            public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
                if (task.getTotalResourceStats().getCpuTimeInNanos() < 300) {
                    return Optional.empty();
                }

                return Optional.of(new TaskCancellation.Reason(this, "limits exceeded", 5));
            }

            @Override
            public Stats currentStats(List<Task> activeTasks) {
                return new MockStats();
            }
        };

        SearchBackpressureSettings settings = spy(new SearchBackpressureSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ));

        SearchBackpressureManager manager = new SearchBackpressureManager(
            settings,
            mockTaskResourceTrackingService,
            mockThreadPool,
            mockTimeNanosSupplier,
            () -> 1.0,  // node in duress
            () -> 0.0,
            List.of(mockTracker)
        );

        manager.run(); manager.run();  // allowing node to be marked 'in duress' from the next iteration
        assertNull(manager.getLastCancelledTaskUsage());

        // Mocking 'settings' with predictable value for task memory usage so that cancellation logic doesn't get skipped.
        long taskHeapUsageBytes = 500;
        when(settings.getSearchHeapUsageThresholdBytes()).thenReturn(taskHeapUsageBytes);

        // Create some active tasks, some of them with high resource usage.
        // 60 low resource usage tasks + 15 high resource usage tasks.
        Map<Long, Task> activeTasks = new HashMap<>();
        for (long i = 0; i < 75; i++) {
            if (i % 5 == 0) {
                activeTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 500, taskHeapUsageBytes));
            } else {
                activeTasks.put(i, createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
            }
        }
        doReturn(activeTasks).when(mockTaskResourceTrackingService).getResourceAwareTasks();

        // There are 15 tasks eligible for cancellation but there haven't been any successful task completions
        // (currentIterationCompletedTasks == 0), so we can only cancel one task.
        manager.run();
        assertEquals(1, manager.getCancellationCount());
        assertEquals(1, manager.getLimitReachedCount());
        assertNotNull(manager.getLastCancelledTaskUsage());

        // Record many task completions so that we are not limited by currentIterationCompletedTasks anymore.
        for (int i = 0; i < 1000; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }

        // Task cancellation rate should still be limited by the token bucket.
        manager.run();
        assertEquals(10, manager.getCancellationCount());
        assertEquals(2, manager.getLimitReachedCount());

        // currentIterationCompletedTasks is reset after each iteration.
        assertEquals(0, manager.getCurrentIterationCompletedTasks());
        for (int i = 0; i < 1000; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }

        // Fast-forward the clock by one second to replenish some tokens.
        mockTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
        manager.run();
        assertEquals(13, manager.getCancellationCount());
        assertEquals(3, manager.getLimitReachedCount());

        // Prepare for the next iteration.
        for (int i = 0; i < 1000; i++) {
            manager.onTaskCompleted(createMockTaskWithResourceStats(SearchShardTask.class, 100, taskHeapUsageBytes));
        }
        mockTime.addAndGet(TimeUnit.SECONDS.toNanos(1));

        // There are only two remaining tasks with high resource usage.
        // limitReachedCount should not be incremented when the number of cancellations in that iteration is less than or equal to the limits.
        manager.run();
        assertEquals(15, manager.getCancellationCount());
        assertEquals(3, manager.getLimitReachedCount());

        // Verify search backpressure stats
        SearchBackpressureStats expectedStats = new SearchBackpressureStats(
            Map.of("mock_tracker", new MockStats()),
            new CancellationStats(
                15,
                Map.of("mock_tracker", 15L),
                3,
                new CancelledTaskStats(500, taskHeapUsageBytes, 2000000000)
            ),
            true,
            true
        );
        assertEquals(expectedStats, manager.nodeStats());
    }
}
