/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Rounding offsets
 *
 * @opensearch.internal
 */
@InternalApi
class OffsetRounding extends Rounding {
    static final byte ID = 3;

    private final Rounding delegate;
    private final long offset;

    OffsetRounding(Rounding delegate, long offset) {
        this.delegate = delegate;
        this.offset = offset;
    }

    OffsetRounding(StreamInput in) throws IOException {
        // Versions before 7.6.0 will never send this type of rounding.
        delegate = Rounding.read(in);
        offset = in.readZLong();
    }

    @Override
    public void innerWriteTo(StreamOutput out) throws IOException {
        delegate.writeTo(out);
        out.writeZLong(offset);
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
        return wrapPreparedRounding(delegate.prepare(minUtcMillis - offset, maxUtcMillis - offset));
    }

    @Override
    public Prepared prepareForUnknown() {
        return wrapPreparedRounding(delegate.prepareForUnknown());
    }

    @Override
    Prepared prepareJavaTime() {
        return wrapPreparedRounding(delegate.prepareJavaTime());
    }

    private Prepared wrapPreparedRounding(Prepared delegatePrepared) {
        return new Prepared() {
            @Override
            public long round(long utcMillis) {
                return delegatePrepared.round(utcMillis - offset) + offset;
            }

            @Override
            public long nextRoundingValue(long utcMillis) {
                return delegatePrepared.nextRoundingValue(utcMillis - offset) + offset;
            }

            @Override
            public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
                return delegatePrepared.roundingSize(utcMillis, timeUnit);
            }
        };
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public Rounding withoutOffset() {
        return delegate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        org.opensearch.common.time.OffsetRounding other = (org.opensearch.common.time.OffsetRounding) obj;
        return delegate.equals(other.delegate) && offset == other.offset;
    }

    @Override
    public String toString() {
        return delegate + " offset by " + offset;
    }
}
