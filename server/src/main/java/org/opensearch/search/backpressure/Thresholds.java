/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.monitor.jvm.JvmStats;

public final class Thresholds {
    public static final double SEARCH_TASK_HEAP_USAGE_THRESHOLD_PERCENTAGE = 0.005;
    public static final long SEARCH_TASK_HEAP_USAGE_THRESHOLD_BYTES = (long) (JvmStats.jvmStats().getMem().getHeapMax().getBytes() * SEARCH_TASK_HEAP_USAGE_THRESHOLD_PERCENTAGE);

    public static final double MAX_TASK_CANCELLATION_PERCENTAGE = 0.1;

    private Thresholds() {
    }
}
