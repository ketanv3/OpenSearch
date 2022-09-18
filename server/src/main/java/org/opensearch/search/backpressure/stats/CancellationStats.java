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
import java.util.Map;

public class CancellationStats implements ToXContentObject, Writeable {
    private final long count;
    private final Map<String, Long> countBreakup;
    private final long limitReachedCount;
    private final Map<String, Long> lastCancelledTaskUsage;

    public CancellationStats(long count, Map<String, Long> countBreakup, long limitReachedCount, Map<String, Long> lastCancelledTaskUsage) {
        this.count = count;
        this.countBreakup = countBreakup;
        this.limitReachedCount = limitReachedCount;
        this.lastCancelledTaskUsage = lastCancelledTaskUsage;
    }

    public CancellationStats(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readMap(StreamInput::readString, StreamInput::readVLong),
            in.readVLong(),
            in.readMap(StreamInput::readString, StreamInput::readVLong)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field("cancellation_count", count)
            .field("cancellation_breakup", countBreakup)
            .field("cancellation_limit_reached_count", limitReachedCount)
            .field("last_cancelled_task", lastCancelledTaskUsage)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeMap(countBreakup, StreamOutput::writeString, StreamOutput::writeVLong);
        out.writeVLong(limitReachedCount);
        out.writeMap(lastCancelledTaskUsage, StreamOutput::writeString, StreamOutput::writeVLong);
    }
}
