/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.Streak;

import java.util.function.BooleanSupplier;

/**
 * NodeResourceUsageTracker is used to check if the node is in duress.
 *
 * @opensearch.internal
 */
public class NodeResourceUsageTracker {
    /**
     * Tracks the number of consecutive breaches.
     */
    private final Streak breaches = new Streak();

    /**
     * Predicate that returns true when the node is in duress.
     */
    private final BooleanSupplier isNodeInDuress;

    public NodeResourceUsageTracker(BooleanSupplier isNodeInDuress) {
        this.isNodeInDuress = isNodeInDuress;
    }

    /**
     * Evaluates the predicate and returns the number of consecutive breaches.
     */
    public int check() {
        return breaches.record(isNodeInDuress.getAsBoolean());
    }
}
