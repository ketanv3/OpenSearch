/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.tasks.Task;

public class CpuUsageTracker implements ResourceUsageTracker {
    public static final float CPU_TIME_NANOS_THRESHOLD = 15 * 1000 * 1000;

    @Override
    public void update(Task task) {
        // nothing to do
    }

    @Override
    public double cancellationScore(Task task) {
        return (task.getTotalResourceStats().getCpuTimeInNanos() >= CPU_TIME_NANOS_THRESHOLD) ? 1 : 0;
    }
}
