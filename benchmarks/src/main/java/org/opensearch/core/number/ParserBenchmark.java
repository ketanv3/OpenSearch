/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.number;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Fork(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 1, time = 5)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class ParserBenchmark {

    private static final int COUNT = 100;
    private static final List<BytesRef> WORKLOAD = new ArrayList<>();

    static {
        Random random = new Random();
        for (int i = 0; i < COUNT; i++) {
            String value = String.valueOf(random.nextLong());
            WORKLOAD.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Benchmark
    @OperationsPerInvocation(COUNT)
    public void baseline(Blackhole bh) {
        for (BytesRef value : WORKLOAD) {
            bh.consume(Long.parseLong(value.utf8ToString()));
        }
    }

    @Benchmark
    @OperationsPerInvocation(COUNT)
    public void candidate(Blackhole bh) {
        for (BytesRef value : WORKLOAD) {
            bh.consume(Numbers.parseLongInlined(value));
        }
    }
}
