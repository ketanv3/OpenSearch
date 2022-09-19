/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.concurrent.TimeUnit;

public class TokenBucket {
    private final double rate;
    private final long burst;
    private long tokens;
    private long lastRefilledAt;

    public TokenBucket(double rate, long burst) {
        this.rate = rate;
        this.burst = burst;
        this.tokens = burst;
        this.lastRefilledAt = System.nanoTime();
    }

    private void refill() {
        long now = System.nanoTime();
        long tokensToAdd = (long) ((now - lastRefilledAt) / TimeUnit.SECONDS.toNanos(1) * rate);

        if (tokensToAdd > 0) {
            lastRefilledAt = now;
            tokens = Math.min(tokens + tokensToAdd, burst);
        }
    }

    public synchronized boolean request() {
        refill();

        if (tokens > 0) {
            tokens--;
            return true;
        }

        return false;
    }
}
