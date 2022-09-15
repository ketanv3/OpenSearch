/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.tasks.Task;

public interface ResourceUsageTracker {
    /**
     * Notifies the tracker to update its state when the task execution completes.
     */
    void update(Task task);

    /**
     * Returns the cancellation score for the given task.
     *
     * A zero score suggests that the task should not be cancelled.
     * A higher score suggests greater possibility of recovering node resources when that task is cancelled.
     */
    double cancellationScore(Task task);
}
