/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import jdk.incubator.vector.*;
import org.opensearch.common.annotation.InternalApi;

import java.util.Arrays;

@InternalApi
public class BidirectionalLinearSearchArrayRounding implements Rounding.Prepared {
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

    private final long[] values;
    private final int max;
    private final Rounding.Prepared delegate;

    public BidirectionalLinearSearchArrayRounding(long[] values, int max, Rounding.Prepared delegate) {
        assert max > 0 : "at least one round-down point must be present";
        this.delegate = delegate;
        int alignedLen = ((max + LONG_SPECIES.length() - 1) / LONG_SPECIES.length()) * LONG_SPECIES.length();
        this.values = new long[alignedLen];
        System.arraycopy(values, 0, this.values, 0, max);
        Arrays.fill(this.values, max, alignedLen, Long.MAX_VALUE);
        this.max = max;
    }

    @Override
    public long round(long utcMillis) {
        Vector<Long> key = LongVector.broadcast(LONG_SPECIES, utcMillis);

        int i = 0;
        for (; i < LONG_SPECIES.loopBound(values.length); i += LONG_SPECIES.length()) {
            Vector<Long> vec = LongVector.fromArray(LONG_SPECIES, values, i);
            VectorMask<Long> msk = key.lt(vec);
            int pos = msk.firstTrue();
            if (pos < msk.length()) {
                return values[i + pos - 1];
            }
        }

        return values[max - 1];
    }

    @Override
    public long nextRoundingValue(long utcMillis) {
        return delegate.nextRoundingValue(utcMillis);
    }

    @Override
    public double roundingSize(long utcMillis, Rounding.DateTimeUnit timeUnit) {
        return delegate.roundingSize(utcMillis, timeUnit);
    }
}
