/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.LocalTimeOffset;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Rounding time intervals
 *
 * @opensearch.internal
 */
@InternalApi
class TimeIntervalRounding extends Rounding {
    private static final Logger logger = LogManager.getLogger(TimeIntervalRounding.class);

    static final byte ID = 2;

    private final long interval;
    private final ZoneId timeZone;

    TimeIntervalRounding(long interval, ZoneId timeZone) {
        if (interval < 1) throw new IllegalArgumentException("Zero or negative time interval not supported");
        this.interval = interval;
        this.timeZone = timeZone;
    }

    TimeIntervalRounding(StreamInput in) throws IOException {
        this(in.readVLong(), in.readZoneId());
    }

    @Override
    public void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(interval);
        out.writeZoneId(timeZone);
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
        long minLookup = minUtcMillis - interval;
        long maxLookup = maxUtcMillis;

        LocalTimeOffset.Lookup lookup = LocalTimeOffset.lookup(timeZone, minLookup, maxLookup);
        if (lookup == null) {
            return prepareJavaTime();
        }
        LocalTimeOffset fixedOffset = lookup.fixedInRange(minLookup, maxLookup);
        if (fixedOffset != null) {
            return new FixedRounding(fixedOffset);
        }
        return new VariableRounding(lookup);
    }

    @Override
    public Prepared prepareForUnknown() {
        LocalTimeOffset offset = LocalTimeOffset.fixedOffset(timeZone);
        if (offset != null) {
            return new FixedRounding(offset);
        }
        return prepareJavaTime();
    }

    @Override
    Prepared prepareJavaTime() {
        return new JavaTimeRounding();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public Rounding withoutOffset() {
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        org.opensearch.common.time.TimeIntervalRounding other = (org.opensearch.common.time.TimeIntervalRounding) obj;
        return Objects.equals(interval, other.interval) && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String toString() {
        return "Rounding[" + interval + " in " + timeZone + "]";
    }

    private long roundKey(long value, long interval) {
        if (value < 0) {
            return (value - interval + 1) / interval;
        } else {
            return value / interval;
        }
    }

    private abstract class TimeIntervalPreparedRounding implements Prepared {
        @Override
        public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
            if (timeUnit.isMillisBased()) {
                return (double) interval / timeUnit.ratio;
            } else {
                throw new IllegalArgumentException(
                    "Cannot use month-based rate unit ["
                        + timeUnit.shortName()
                        + "] with fixed interval based histogram, only week, day, hour, minute and second are supported for "
                        + "this histogram"
                );
            }
        }
    }

    /**
     * Rounds to down inside of a time zone with an "effectively fixed"
     * time zone. A time zone can be "effectively fixed" if:
     * <ul>
     * <li>It is UTC</li>
     * <li>It is a fixed offset from UTC at all times (UTC-5, America/Phoenix)</li>
     * <li>It is fixed over the entire range of dates that will be rounded</li>
     * </ul>
     */
    private class FixedRounding extends TimeIntervalPreparedRounding {
        private final LocalTimeOffset offset;

        FixedRounding(LocalTimeOffset offset) {
            this.offset = offset;
        }

        @Override
        public long round(long utcMillis) {
            return offset.localToUtcInThisOffset(roundKey(offset.utcToLocalTime(utcMillis), interval) * interval);
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            // TODO this is used in date range's collect so we should optimize it too
            return new JavaTimeRounding().nextRoundingValue(utcMillis);
        }
    }

    /**
     * Rounds down inside of any time zone, even if it is not
     * "effectively fixed". See {@link FixedRounding} for a description of
     * "effectively fixed".
     */
    private class VariableRounding extends TimeIntervalPreparedRounding implements LocalTimeOffset.Strategy {
        private final LocalTimeOffset.Lookup lookup;

        VariableRounding(LocalTimeOffset.Lookup lookup) {
            this.lookup = lookup;
        }

        @Override
        public long round(long utcMillis) {
            LocalTimeOffset offset = lookup.lookup(utcMillis);
            return offset.localToUtc(roundKey(offset.utcToLocalTime(utcMillis), interval) * interval, this);
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            // TODO this is used in date range's collect so we should optimize it too
            return new JavaTimeRounding().nextRoundingValue(utcMillis);
        }

        @Override
        public long inGap(long localMillis, LocalTimeOffset.Gap gap) {
            return gap.startUtcMillis();
        }

        @Override
        public long beforeGap(long localMillis, LocalTimeOffset.Gap gap) {
            return gap.previous().localToUtc(localMillis, this);
        }

        @Override
        public long inOverlap(long localMillis, LocalTimeOffset.Overlap overlap) {
            // Convert the overlap at this offset because that'll produce the largest result.
            return overlap.localToUtcInThisOffset(localMillis);
        }

        @Override
        public long beforeOverlap(long localMillis, LocalTimeOffset.Overlap overlap) {
            return overlap.previous().localToUtc(roundKey(overlap.firstNonOverlappingLocalTime() - 1, interval) * interval, this);
        }
    }

    /**
     * Rounds down inside of any time zone using {@link LocalDateTime}
     * directly. It'll be slower than {@link VariableRounding} and much
     * slower than {@link FixedRounding}. We use it when we don' have an
     * "effectively fixed" time zone and we can't get a
     * {@link LocalTimeOffset.Lookup}. We might not be able to get one
     * because:
     * <ul>
     * <li>We don't know how to look up the minimum and maximum dates we
     * are going to round.</li>
     * <li>We expect to round over thousands and thousands of years worth
     * of dates with the same {@link Prepared} instance.</li>
     * </ul>
     */
    private class JavaTimeRounding extends TimeIntervalPreparedRounding {
        @Override
        public long round(long utcMillis) {
            final Instant utcInstant = Instant.ofEpochMilli(utcMillis);
            final LocalDateTime rawLocalDateTime = LocalDateTime.ofInstant(utcInstant, timeZone);

            // a millisecond value with the same local time, in UTC, as `utcMillis` has in `timeZone`
            final long localMillis = utcMillis + timeZone.getRules().getOffset(utcInstant).getTotalSeconds() * 1000;
            assert localMillis == rawLocalDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            final long roundedMillis = roundKey(localMillis, interval) * interval;
            final LocalDateTime roundedLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(roundedMillis), ZoneOffset.UTC);

            // Now work out what roundedLocalDateTime actually means
            final List<ZoneOffset> currentOffsets = timeZone.getRules().getValidOffsets(roundedLocalDateTime);
            if (currentOffsets.isEmpty() == false) {
                // There is at least one instant with the desired local time. In general the desired result is
                // the latest rounded time that's no later than the input time, but this could involve rounding across
                // a timezone transition, which may yield the wrong result
                final ZoneOffsetTransition previousTransition = timeZone.getRules().previousTransition(utcInstant.plusMillis(1));
                for (int offsetIndex = currentOffsets.size() - 1; 0 <= offsetIndex; offsetIndex--) {
                    final OffsetDateTime offsetTime = roundedLocalDateTime.atOffset(currentOffsets.get(offsetIndex));
                    final Instant offsetInstant = offsetTime.toInstant();
                    if (previousTransition != null && offsetInstant.isBefore(previousTransition.getInstant())) {
                        /*
                         * Rounding down across the transition can yield the
                         * wrong result. It's best to return to the transition
                         * time and round that down.
                         */
                        return round(previousTransition.getInstant().toEpochMilli() - 1);
                    }

                    if (utcInstant.isBefore(offsetTime.toInstant()) == false) {
                        return offsetInstant.toEpochMilli();
                    }
                }

                final OffsetDateTime offsetTime = roundedLocalDateTime.atOffset(currentOffsets.get(0));
                final Instant offsetInstant = offsetTime.toInstant();
                assert false : this + " failed to round " + utcMillis + " down: " + offsetInstant + " is the earliest possible";
                return offsetInstant.toEpochMilli(); // TODO or throw something?
            } else {
                // The desired time isn't valid because within a gap, so just return the start of the gap
                ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().getTransition(roundedLocalDateTime);
                return zoneOffsetTransition.getInstant().toEpochMilli();
            }
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            /*
             * Ok. I'm not proud of this, but it gets the job done. So here is the deal:
             * its super important that nextRoundingValue be *exactly* the next rounding
             * value. And I can't come up with a nice way to use the java time API to figure
             * it out. Thus, we treat "round" like a black box here and run a kind of whacky
             * binary search, newton's method hybrid. We don't have a "slope" so we can't do
             * a "real" newton's method, so we just sort of cut the diff in half. As janky
             * as it looks, it tends to get the job done in under four iterations. Frankly,
             * `round(round(utcMillis) + interval)` is usually a good guess so we mostly get
             * it in a single iteration. But daylight savings time and other janky stuff can
             * make it less likely.
             */
            long prevRound = round(utcMillis);
            long increment = interval;
            long from = prevRound;
            int iterations = 0;
            while (++iterations < 100) {
                from += increment;
                long rounded = round(from);
                boolean highEnough = rounded > prevRound;
                if (false == highEnough) {
                    if (increment < 0) {
                        increment = -increment / 2;
                    }
                    continue;
                }
                long roundedRoundedDown = round(rounded - 1);
                boolean tooHigh = roundedRoundedDown > prevRound;
                if (tooHigh) {
                    if (increment > 0) {
                        increment = -increment / 2;
                    }
                    continue;
                }
                assert highEnough && (false == tooHigh);
                assert roundedRoundedDown == prevRound;
                if (iterations > 3 && logger.isDebugEnabled()) {
                    logger.debug(
                        "Iterated {} time for {} using {}",
                        iterations,
                        utcMillis,
                        org.opensearch.common.time.TimeIntervalRounding.this.toString()
                    );
                }
                return rounded;
            }
            /*
             * After 100 iterations we still couldn't settle on something! Crazy!
             * The most I've seen in tests is 20 and its usually 1 or 2. If we're
             * not in a test let's log something and round from our best guess.
             */
            assert false : String.format(
                Locale.ROOT,
                "Expected to find the rounding in 100 iterations but didn't for [%d] with [%s]",
                utcMillis,
                org.opensearch.common.time.TimeIntervalRounding.this.toString()
            );
            logger.debug(
                "Expected to find the rounding in 100 iterations but didn't for {} using {}",
                utcMillis,
                org.opensearch.common.time.TimeIntervalRounding.this.toString()
            );
            return round(from);
        }
    }
}
