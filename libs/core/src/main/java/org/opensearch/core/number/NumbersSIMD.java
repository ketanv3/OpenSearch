/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.number;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

@SuppressWarnings("fallthrough")
public class NumbersSIMD {
    private static final VarHandle VH_LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle VH_INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle VH_SHORT_LE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final long[] POWERS = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};

    /**
     * Parses a long value from its UTF-8 encoded string representation.
     */
    public static long parseLong(byte[] utf8, int offset, int length) {
        boolean isNegative = false;

        if (utf8[offset] < '0') {
            if (utf8[offset] == '-') {
                isNegative = true;
            } else if (utf8[offset] != '+') {
                throw new NumberFormatException();
            }
            length -= 1;
            offset += 1;
        }

        long result = 0;
        long chunk;

        switch (length) {
            case 19:
            case 18:
            case 17:
                // Read 8 digits
                chunk = parse(read8(utf8, offset));
                result = chunk;
                offset += 8;
                length -= 8;
            case 16:
            case 15:
            case 14:
            case 13:
            case 12:
            case 11:
            case 10:
            case 9:
                // Read 8 digits
                chunk = parse(read8(utf8, offset));
                result = POWERS[8] * result + chunk;
                offset += 8;
                length -= 8;
            case 8:
            case 7:
            case 6:
            case 5:
            case 4:
            case 3:
            case 2:
            case 1:
                // Read between 1-8 digits (inclusive)
                chunk = parse(readTail(utf8, offset, length));
                result = POWERS[length] * result + chunk;
                return isNegative ? -result : result;
            case 0:
            default:
                throw new NumberFormatException();
        }
    }

    /**
     * Parses the output of readX and returns its decimal representation.
     * This uses a technique called "SIMD within a register" (<a href="https://en.wikipedia.org/wiki/SWAR">SWAR</a>).
     */
    private static long parse(long chunk) {
        // Subtract the character '0' from all characters.
        long val = chunk - 0x3030303030303030L;

        // Create a predicate for all bytes which are greater than '0' (0x30).
        // The predicate is true if the hsb of a byte is set: (predicate & 0x80) != 0.
        if ((((chunk + 0x4646464646464646L) | val) & 0x8080808080808080L) != 0L) {
            throw new NumberFormatException();
        }

        // Bit-twiddling to convert digits (bytes) in little-endian order to a decimal number.
        final long mask = 0x000000FF000000FFL;
        final long mul1 = 0x000F424000000064L;
        final long mul2 = 0x0000271000000001L;
        val = (val * 10) + (val >>> 8);
        return (((val & mask) * mul1) + (((val >>> 16) & mask) * mul2)) >>> 32;
    }

    /**
     * Reads 8 digits in LE order and returns a long.
     */
    private static long read8(byte[] utf8, int offset) {
        return (long) VH_LONG_LE.get(utf8, offset);
    }

    /**
     * Reads 4 digits in LE order and returns a long.
     */
    private static long read4(byte[] utf8, int offset) {
        return (int) VH_INT_LE.get(utf8, offset) & 0xFFFFFFFFL;
    }

    /**
     * Reads 2 digits in LE order and returns a long.
     */
    private static long read2(byte[] utf8, int offset) {
        return (short) VH_SHORT_LE.get(utf8, offset) & 0xFFFFL;
    }

    /**
     * Reads 1 digit in LE order and returns a long.
     */
    private static long read1(byte[] utf8, int offset) {
        return utf8[offset] & 0xFFL;
    }

    /**
     * Reads between 1-8 digits (inclusive) in LE order and returns a long.
     */
    private static long readTail(byte[] utf8, int offset, int length) {
        switch (length) {
            case 1:
                return (read1(utf8, offset) << 56) | 0x30303030303030L;
            case 2:
                return (read2(utf8, offset) << 48) | 0x303030303030L;
            case 3:
                return (read2(utf8, offset) << 40) | (read1(utf8, offset + 2) << 56) | 0x3030303030L;
            case 4:
                return (read4(utf8, offset) << 32) | 0x30303030L;
            case 5:
                return (read4(utf8, offset) << 24) | (read1(utf8, offset + 4) << 56) | 0x303030L;
            case 6:
                return (read4(utf8, offset) << 16) | (read2(utf8, offset + 4) << 48) | 0x3030L;
            case 7:
                return (read4(utf8, offset) << 8) | (read4(utf8, offset + 3) << 32) | 0x30L;
            case 8:
                return read8(utf8, offset);
            default:
                throw new IllegalArgumentException("unsupported length");
        }
    }
}
