/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * TokenBucket is used to limit the number of operations at a constant rate while allowing for short bursts.
 */
public class TokenBucket {
    private static final double SECOND_TO_NANOS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Defines the rate at which tokens are added to the bucket each second.
     */
    private final double rate;

    /**
     * Defines the maximum number of operations that can be performed before the bucket runs out of tokens.
     */
    private final double burst;

    private final LongSupplier timeNanosSupplier;
    private double tokens;
    private long lastRefilledAt;

    public TokenBucket(double rate, double burst, LongSupplier timeNanosSupplier) {
        this.rate = rate;
        this.burst = burst;
        this.timeNanosSupplier = timeNanosSupplier;
        this.tokens = burst;
        this.lastRefilledAt = timeNanosSupplier.getAsLong();
    }

    /**
     * Refills the token bucket.
     */
    private void refill() {
        long now = timeNanosSupplier.getAsLong();
        double incr = ((now - lastRefilledAt) / SECOND_TO_NANOS) * rate;
        tokens = Math.min(tokens + incr, burst);
        lastRefilledAt = now;
    }

    /**
     * If there are >= 1 tokens, it requests/deducts one token and returns true.
     * Otherwise, returns false and leaves the bucket untouched.
     */
    public synchronized boolean request() {
        refill();

        if (tokens >= 1) {
            tokens -= 1.0;
            return true;
        }

        return false;
    }
}
