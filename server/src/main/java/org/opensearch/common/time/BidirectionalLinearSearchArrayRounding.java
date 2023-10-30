/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.opensearch.common.annotation.InternalApi;

/**
 * Implementation of {@link Rounding.Prepared} using pre-calculated "round down" points.
 *
 * <p>
 * It uses linear search to find the greatest round-down point less than or equal to the given timestamp.
 * For small inputs (&le; 64 elements), this can be much faster than binary search as it avoids the penalty of
 * branch mispredictions and pipeline stalls, and accesses memory sequentially.
 *
 * <p>
 * It uses "meet in the middle" linear search to avoid the worst case scenario when the desired element is present
 * at either side of the array. This is helpful for time-series data where velocity increases over time, so more
 * documents are likely to find a greater timestamp which is likely to be present on the right end of the array.
 *
 * @opensearch.internal
 */
@InternalApi
class BidirectionalLinearSearchArrayRounding implements Rounding.Prepared {
    private final long[] ascending;
    private final long[] descending;
    private final Rounding.Prepared delegate;

    BidirectionalLinearSearchArrayRounding(long[] values, int max, Rounding.Prepared delegate) {
        assert max > 0 : "at least one round-down point must be present";
        this.delegate = delegate;
        int len = (max + 1) >>> 1; // rounded-up to handle odd number of values
        ascending = new long[len];
        descending = new long[len];

        for (int i = 0; i < len; i++) {
            ascending[i] = values[i];
            descending[i] = values[max - i - 1];
        }
    }

    @Override
    public long round(long utcMillis) {
        int i = 0;
        for (; i < ascending.length; i++) {
            if (descending[i] <= utcMillis) {
                return descending[i];
            }
            if (ascending[i] > utcMillis) {
                assert i > 0 : "utcMillis must be after " + ascending[0];
                return ascending[i - 1];
            }
        }
        return ascending[i - 1];
    }

    @Override
    public long nextRoundingValue(long utcMillis) {
        return delegate.nextRoundingValue(utcMillis);
    }

    @Override
    public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
        return delegate.roundingSize(utcMillis, timeUnit);
    }
}
