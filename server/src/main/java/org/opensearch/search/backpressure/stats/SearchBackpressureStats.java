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
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class SearchBackpressureStats implements ToXContentFragment, Writeable {
    private final Map<String, Map<String, Double>> searchShardTaskCurrentStats;
    private final CancellationStats searchShardTaskCancellationStats;
    private final boolean enabled;
    private final boolean enforced;


    public SearchBackpressureStats(
        Map<String, Map<String, Double>> searchShardTaskCurrentStats,
        CancellationStats searchShardTaskCancellationStats,
        boolean enabled,
        boolean enforced
    ) {
        this.searchShardTaskCurrentStats = searchShardTaskCurrentStats;
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
        this.enabled = enabled;
        this.enforced = enforced;
    }

    public SearchBackpressureStats(StreamInput in) throws IOException {
        this(
            in.readMap(StreamInput::readString, i -> i.readMap(StreamInput::readString, StreamInput::readDouble)),
            new CancellationStats(in),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject("search_backpressure")
            .startObject("current_stats")
            .field("search_shard_task", searchShardTaskCurrentStats)
            .endObject()
            .startObject("cancellation_stats")
            .field("search_shard_task", searchShardTaskCancellationStats)
            .endObject()
            .field("enabled", enabled)
            .field("enforced", enforced)
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(searchShardTaskCurrentStats, StreamOutput::writeString, (o, stats) -> o.writeMap(stats, StreamOutput::writeString, StreamOutput::writeDouble));
        searchShardTaskCancellationStats.writeTo(out);
        out.writeBoolean(enabled);
        out.writeBoolean(enforced);
    }
}
