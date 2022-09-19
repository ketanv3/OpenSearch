/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.search.backpressure.TaskCancellation;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ResourceUsageTracker {
    private final AtomicLong cancellations = new AtomicLong();

    public abstract String name();

    /**
     * Notifies the tracker to update its state when the task execution completes.
     */
    public abstract void update(Task task);

    /**
     * Returns the cancellation reason for the given task.
     *
     * If the score is zero, then the task hasn't breached thresholds and should not be cancelled.
     * A higher score suggests greater possibility of recovering the node when that task is cancelled.
     */
    public abstract Optional<TaskCancellation.Reason> cancellationReason(Task task);

    /**
     * Returns the current state of the tracker as seen in the _stats API.
     */
    public abstract Stats currentStats(List<Task> activeTasks);

    public long incrementCancellations() {
        return cancellations.incrementAndGet();
    }

    public long getCancellations() {
        return cancellations.get();
    }

    public interface Stats extends ToXContentObject, Writeable {}
}
