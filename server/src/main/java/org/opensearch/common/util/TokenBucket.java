/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.function.LongSupplier;

/**
 * TokenBucket is used to limit the number of operations at a constant rate while allowing for short bursts.
 */
public class TokenBucket {
    /**
     * Defines a monotonically increasing counter.
     *
     * Some examples:
     * 1. clock = System::nanoTime - to perform rate-limiting per unit time.
     * 2. clock = AtomicLong::incrementAndGet - to perform rate-limiting per unit operation.
     */
    private final LongSupplier clock;

    /**
     * Defines the number of tokens added to the bucket per 'clock' cycle.
     */
    private final double rate;

    /**
     * Defines the capacity as well as the maximum number of operations that can be performed per 'clock' cycle before
     * the bucket runs out of tokens.
     */
    private final double burst;

    private double tokens;

    private long lastRefilledAt;

    public TokenBucket(LongSupplier clock, double rate, double burst) {
        if (rate <= 0.0) {
            throw new IllegalArgumentException("rate must be greater than zero");
        }

        if (burst <= 0.0) {
            throw new IllegalArgumentException("burst must be greater than zero");
        }

        this.clock = clock;
        this.rate = rate;
        this.burst = burst;
        this.tokens = burst;
        this.lastRefilledAt = clock.getAsLong();
    }

    /**
     * Refills the token bucket.
     */
    private void refill() {
        long now = clock.getAsLong();
        double incr = (now - lastRefilledAt) * rate;
        tokens = Math.min(tokens + incr, burst);
        lastRefilledAt = now;
    }

    /**
     * If there are >= 1 tokens, it requests/deducts one token and returns true.
     * Otherwise, returns false and leaves the bucket untouched.
     */
    public synchronized boolean request() {
        refill();

        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }

        return false;
    }
}
