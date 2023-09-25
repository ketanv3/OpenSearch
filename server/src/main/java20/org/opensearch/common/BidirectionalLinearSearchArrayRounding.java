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

@InternalApi
public class BidirectionalLinearSearchArrayRounding implements Rounding.Prepared {
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final int LANES = LONG_SPECIES.length();

    private final int len;
    private final long[] ascending;
    private final long[] descending;
    private final Rounding.Prepared delegate;

    public BidirectionalLinearSearchArrayRounding(long[] values, int max, Rounding.Prepared delegate) {
        assert max > 0 : "at least one round-down point must be present";
        this.delegate = delegate;
        int len = (max + 1) >>> 1; // rounded-up to handle odd number of values
        int lenAligned = ((len + LANES - 1) / LANES) * LANES; // align the length to the nearest multiple of LANES
        this.len = len;
        ascending = new long[lenAligned];
        descending = new long[lenAligned];

        for (int i = 0; i < len; i++) {
            ascending[i] = values[i];
            descending[i] = values[max - i - 1];
        }

        for (int i = len; i < lenAligned; i++) {
            ascending[i] = Long.MIN_VALUE;
            descending[i] = Long.MAX_VALUE;
        }
    }

    @Override
    public long round(long utcMillis) {
        Vector<Long> mask = LongVector.broadcast(LONG_SPECIES, utcMillis);

        int i = 0;
        for (; i < LONG_SPECIES.loopBound(ascending.length); i += LANES) {
            Vector<Long> d = LongVector.fromArray(LONG_SPECIES, descending, i);
            VectorMask<Long> md = d.compare(VectorOperators.LE, mask);
            int pd = md.firstTrue();
            if (pd < md.length()) {
                return descending[i + pd];
            }

            Vector<Long> a = LongVector.fromArray(LONG_SPECIES, ascending, i);
            VectorMask<Long> ma = a.compare(VectorOperators.GT, mask);
            int pa = ma.firstTrue();
            if (pa < ma.length()) {
                return ascending[i + pa - 1];
            }
        }
        return ascending[len - 1];
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
