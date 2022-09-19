/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

public class SearchBackpressureStatsTests extends AbstractWireSerializingTestCase<SearchBackpressureStats> {
    @Override
    protected Writeable.Reader<SearchBackpressureStats> instanceReader() {
        return SearchBackpressureStats::new;
    }

    @Override
    protected SearchBackpressureStats createTestInstance() {
        return new SearchBackpressureStats(
            new MapBuilder<String, Map<String, Double>>()
                .put("foo", new MapBuilder<String, Double>().put("current_avg", 12.0).put("current_max", 50.0).immutableMap())
                .put("bar", new MapBuilder<String, Double>().put("rolling_avg", 10.0).immutableMap())
                .immutableMap(),
            CancellationStatsTests.randomInstance(),
            randomBoolean(),
            randomBoolean()
        );
    }
}
