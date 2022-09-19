/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class CancelledTaskStats implements ToXContentObject, Writeable {
    private final long memoryUsageBytes;
    private final long cpuUsageNanos;
    private final long elapsedTimeNanos;

    public CancelledTaskStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    public CancelledTaskStats(long memoryUsageBytes, long cpuUsageNanos, long elapsedTimeNanos) {
        this.memoryUsageBytes = memoryUsageBytes;
        this.cpuUsageNanos = cpuUsageNanos;
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field("heap_usage_bytes", memoryUsageBytes)
            .field("cpu_usage_nanos", cpuUsageNanos)
            .field("elapsed_time_nanos", elapsedTimeNanos)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memoryUsageBytes);
        out.writeVLong(cpuUsageNanos);
        out.writeVLong(elapsedTimeNanos);
    }
}
