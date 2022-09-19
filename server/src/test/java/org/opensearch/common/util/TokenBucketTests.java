/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class TokenBucketTests extends OpenSearchTestCase {
    public void testTokenBucket() {
        AtomicLong mockTimeNanos = new AtomicLong();
        LongSupplier mockTimeNanosSupplier = mockTimeNanos::get;

        // Token bucket that refills at 2 tokens/second and allows short bursts up to 3 operations.
        TokenBucket tokenBucket = new TokenBucket(2, 3, mockTimeNanosSupplier);

        // Three operations succeed, fourth fails.
        assertTrue(tokenBucket.request());
        assertTrue(tokenBucket.request());
        assertTrue(tokenBucket.request());
        assertFalse(tokenBucket.request());

        // Clock moves ahead by one second. Two operations succeed, third fails.
        mockTimeNanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
        assertTrue(tokenBucket.request());
        assertTrue(tokenBucket.request());
        assertFalse(tokenBucket.request());

        // Clock moves ahead by half a second. One operation succeeds, second fails.
        mockTimeNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
        assertTrue(tokenBucket.request());
        assertFalse(tokenBucket.request());
    }
}
