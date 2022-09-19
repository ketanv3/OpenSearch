/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.opensearch.search.backpressure.trackers.ResourceUsageTrackerTestsHelper.createMockTaskWithResourceStats;

public class ElapsedTimeTrackerTests extends OpenSearchTestCase {

    public void testEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(1, 1);
        ElapsedTimeTracker tracker = new ElapsedTimeTracker(() -> ElapsedTimeTracker.ELAPSED_TIME_NANOS_THRESHOLD + 1);

        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertTrue(reason.isPresent());
        assertSame(tracker, reason.get().getTracker());
        assertEquals(1, reason.get().getCancellationScore());
        assertEquals("elapsed time exceeded", reason.get().getMessage());
    }

    public void testNotEligibleForCancellation() {
        Task task = createMockTaskWithResourceStats(1, 1);
        ElapsedTimeTracker tracker = new ElapsedTimeTracker(() -> ElapsedTimeTracker.ELAPSED_TIME_NANOS_THRESHOLD - 1);

        Optional<TaskCancellation.Reason> reason = tracker.cancellationReason(task);
        assertFalse(reason.isPresent());
    }
}
