/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.search.backpressure.Thresholds;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.opensearch.search.backpressure.TestHelpers.createMockTaskWithResourceStats;

public class HeapUsageTrackerTests extends OpenSearchTestCase {

    public void testEligibleForCancellation() {
        HeapUsageTracker tracker = new HeapUsageTracker(() -> 100L, () -> 2.0);
        Task task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 50);

        // record enough observations to make the moving average 'ready'
        for (int i = 0; i < 100; i++) {
            tracker.update(task);
        }

        // task that has heap usage >= SEARCH_HEAP_USAGE_THRESHOLD_BYTES and (moving average * variance).
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 500);
        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertTrue(reason.isPresent());
        assertSame(tracker, reason.get().getTracker());
        assertEquals(10, reason.get().getCancellationScore());
        assertEquals("heap usage exceeded", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task;
        Optional<TaskCancellation.Reason> reason;
        HeapUsageTracker tracker = new HeapUsageTracker(() -> 100L, () -> 2.0);

        // task with heap usage < SEARCH_TASK_HEAP_USAGE_THRESHOLD_BYTES
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, 99);

        // not enough observations
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());

        // record enough observations to make the moving average 'ready'
        for (int i = 0; i < 100; i++) {
            tracker.update(task);
        }

        // task with heap usage < SEARCH_TASK_HEAP_USAGE_THRESHOLD_BYTES should not be cancelled
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());

        // task with heap usage between SEARCH_TASK_HEAP_USAGE_THRESHOLD_BYTES (inclusive) and (moving average * variance) (exclusive) should not be cancelled.
        double allowedHeapUsage = 99.0 * 2.0;
        task = createMockTaskWithResourceStats(SearchShardTask.class, 1, randomLongBetween(99, (long) allowedHeapUsage - 1));
        reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
