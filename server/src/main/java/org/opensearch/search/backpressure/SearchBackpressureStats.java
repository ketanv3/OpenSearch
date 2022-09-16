/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class SearchBackpressureStats implements Writeable, ToXContentFragment {
    public SearchBackpressureStats() {

    }

    public SearchBackpressureStats(StreamInput in) {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_backpressure");

        builder.startObject("current_stats");
        builder.startObject("search_shard_task");
        builder.startObject("heap_memory_consumed_bytes")
            .field("current_avg", 0)
            .field("current_max", 0)
            .field("rolling_avg", 0)
            .endObject();
        builder.startObject("cpu_time_consumed_nanos")
            .field("current_avg", 0)
            .field("current_max", 0)
            .endObject();
        builder.startObject("elapsed_time_nanos")
            .field("current_avg", 0)
            .field("current_max", 0)
            .endObject();
        builder.endObject();
        builder.endObject();

        builder.startObject("cancellation_stats");
        builder.startObject("search_shard_task");
        builder.field("cancellation_count", 0);
        builder.startObject("last_cancelled_task");
        builder.field("memory_consumed_bytes", 0);
        builder.field("cpu_consumed_nanos", 0);
        builder.field("elapsed_time_nanos", 0);
        builder.endObject();
        builder.endObject();
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
