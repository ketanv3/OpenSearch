/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.tasks.Task;

public class ElapsedTimeTracker implements ResourceUsageTracker {
    public static final float ELAPSED_TIME_MILLIS_THRESHOLD = 1 * 1000;

    @Override
    public void update(Task task) {

    }

    @Override
    public double cancellationScore(Task task) {
        return (System.currentTimeMillis() - task.getStartTime() >= ELAPSED_TIME_MILLIS_THRESHOLD) ? 1 : 0;
    }
}
