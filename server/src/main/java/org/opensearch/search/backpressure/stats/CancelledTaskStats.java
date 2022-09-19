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
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CancelledTaskStats implements ToXContentObject, Writeable {
    private final long heapUsageBytes;
    private final long cpuUsageNanos;
    private final long elapsedTimeNanos;

    public CancelledTaskStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    public CancelledTaskStats(long heapUsageBytes, long cpuUsageNanos, long elapsedTimeNanos) {
        this.heapUsageBytes = heapUsageBytes;
        this.cpuUsageNanos = cpuUsageNanos;
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .humanReadableField("heap_usage_bytes", "heap_usage", new ByteSizeValue(heapUsageBytes))
            .humanReadableField("cpu_usage_nanos", "cpu_usage", new TimeValue(cpuUsageNanos, TimeUnit.NANOSECONDS))
            .humanReadableField("elapsed_time_nanos", "elapsed_time", new TimeValue(elapsedTimeNanos, TimeUnit.NANOSECONDS))
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(heapUsageBytes);
        out.writeVLong(cpuUsageNanos);
        out.writeVLong(elapsedTimeNanos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelledTaskStats that = (CancelledTaskStats) o;
        return heapUsageBytes == that.heapUsageBytes && cpuUsageNanos == that.cpuUsageNanos && elapsedTimeNanos == that.elapsedTimeNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(heapUsageBytes, cpuUsageNanos, elapsedTimeNanos);
    }
}
