/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.number;

import org.apache.lucene.util.BytesRef;

public class Numbers {
    private static final int HALF_MASK = 0x3FF;
    private static final int UNI_MAX_BMP = 0x0000FFFF;

    public static Long parseLong(BytesRef bytesRef) {
        return parse(bytesRef.bytes, bytesRef.offset, bytesRef.length, LONG_PARSER);
    }

    private static <T extends Number> T parse(byte[] utf8, int offset, int length, Parser<T> parser) {
        final int limit = offset + length;
        State state = parser.init();

        while (offset < limit) {
            int b = utf8[offset++] & 0xff;
            if (b < 0xc0) {
                assert b < 0x80;
                state = state.next((char) b);
            } else if (b < 0xe0) {
                state = state.next((char) (((b & 0x1f) << 6) + (utf8[offset++] & 0x3f)));
            } else if (b < 0xf0) {
                state = state.next((char) (((b & 0xf) << 12) + ((utf8[offset] & 0x3f) << 6) + (utf8[offset + 1] & 0x3f)));
                offset += 2;
            } else {
                assert b < 0xf8 : "b = 0x" + Integer.toHexString(b);
                int ch =
                    ((b & 0x7) << 18)
                        + ((utf8[offset] & 0x3f) << 12)
                        + ((utf8[offset + 1] & 0x3f) << 6)
                        + (utf8[offset + 2] & 0x3f);
                offset += 3;
                if (ch < UNI_MAX_BMP) {
                    state = state.next((char) ch);
                } else {
                    int chHalf = ch - 0x0010000;
                    state = state.next((char) ((chHalf >> 10) + 0xD800));
                    state = state.next((char) ((chHalf & HALF_MASK) + 0xDC00));
                }
            }
        }

        return parser.result();
    }

    private interface Parser<T extends Number> {
        State init();
        T result();
        void reset();
    }

    @FunctionalInterface
    private interface State {
        State next(char ch);
    }

    private static final Parser<Long> LONG_PARSER = new Parser<>() {
        private boolean isNegative;
        private long result;
        private long limit;
        private long multmin;

        private final State readNumber = new State() {
            @Override
            public State next(char ch) {
                int digit = Character.digit(ch, 10);
                if (digit < 0 || result < multmin) {
                    throw new NumberFormatException();
                }

                result *= 10;
                if (result < limit + digit) {
                    throw new NumberFormatException();
                }

                result -= digit;
                return this;
            }
        };

        private final State readSign = new State() {
            @Override
            public State next(char ch) {
                if (ch < '0') {
                    if (ch == '-') {
                        isNegative = true;
                        limit = Long.MIN_VALUE;
                        multmin = limit / 10;
                    } else if (ch != '+') {
                        throw new NumberFormatException();
                    }
                    return readNumber;
                }
                return readNumber.next(ch);
            }
        };

        @Override
        public State init() {
            reset();
            return readSign;
        }

        @Override
        public Long result() {
            return isNegative ? result : -result;
        }

        @Override
        public void reset() {
            result = 0;
            isNegative = false;
            limit = -Long.MAX_VALUE;
            multmin = limit / 10;
        }
    };
}
