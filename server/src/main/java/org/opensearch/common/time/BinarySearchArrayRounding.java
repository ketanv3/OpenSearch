/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.opensearch.common.annotation.InternalApi;

import java.util.Arrays;

/**
 * Implementation of {@link Rounding.Prepared} using pre-calculated "round down" points.
 *
 * <p>
 * It uses binary search to find the greatest round-down point less than or equal to the given timestamp.
 *
 * @opensearch.internal
 */
@InternalApi
class BinarySearchArrayRounding implements Rounding.Prepared {
    private final long[] values;
    private final int max;
    private final Rounding.Prepared delegate;

    BinarySearchArrayRounding(long[] values, int max, Rounding.Prepared delegate) {
        assert max > 0 : "at least one round-down point must be present";
        this.values = values;
        this.max = max;
        this.delegate = delegate;
    }

    @Override
    public long round(long utcMillis) {
        assert values[0] <= utcMillis : "utcMillis must be after " + values[0];
        int idx = Arrays.binarySearch(values, 0, max, utcMillis);
        assert idx != -1 : "The insertion point is before the array! This should have tripped the assertion above.";
        assert -1 - idx <= values.length : "This insertion point is after the end of the array.";
        if (idx < 0) {
            idx = -2 - idx;
        }
        return values[idx];
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
