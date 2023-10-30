/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.opensearch.common.LocalTimeOffset;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalQueries;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Objects;

/**
 * Rounding time units
 *
 * @opensearch.internal
 */
@InternalApi
class TimeUnitRounding extends Rounding {
    static final byte ID = 1;

    private final DateTimeUnit unit;
    private final ZoneId timeZone;
    private final boolean unitRoundsToMidnight;

    TimeUnitRounding(DateTimeUnit unit, ZoneId timeZone) {
        this.unit = unit;
        this.timeZone = timeZone;
        this.unitRoundsToMidnight = this.unit.getField().getBaseUnit().getDuration().toMillis() > 3600000L;
    }

    TimeUnitRounding(StreamInput in) throws IOException {
        this(DateTimeUnit.resolve(in.readByte()), in.readZoneId());
    }

    @Override
    public void innerWriteTo(StreamOutput out) throws IOException {
        out.writeByte(unit.getId());
        out.writeZoneId(timeZone);
    }

    @Override
    public byte id() {
        return ID;
    }

    private LocalDateTime truncateLocalDateTime(LocalDateTime localDateTime) {
        switch (unit) {
            case SECOND_OF_MINUTE:
                return localDateTime.withNano(0);

            case MINUTES_OF_HOUR:
                return LocalDateTime.of(
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    0,
                    0
                );

            case HOUR_OF_DAY:
                return LocalDateTime.of(
                    localDateTime.getYear(),
                    localDateTime.getMonth(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    0,
                    0
                );

            case DAY_OF_MONTH:
                LocalDate localDate = localDateTime.query(TemporalQueries.localDate());
                return localDate.atStartOfDay();

            case WEEK_OF_WEEKYEAR:
                return LocalDateTime.of(localDateTime.toLocalDate(), LocalTime.MIDNIGHT).with(ChronoField.DAY_OF_WEEK, 1);

            case MONTH_OF_YEAR:
                return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonthValue(), 1, 0, 0);

            case QUARTER_OF_YEAR:
                return LocalDateTime.of(localDateTime.getYear(), localDateTime.getMonth().firstMonthOfQuarter(), 1, 0, 0);

            case YEAR_OF_CENTURY:
                return LocalDateTime.of(LocalDate.of(localDateTime.getYear(), 1, 1), LocalTime.MIDNIGHT);

            default:
                throw new IllegalArgumentException("NOT YET IMPLEMENTED for unit " + unit);
        }
    }

    @Override
    public Prepared prepare(long minUtcMillis, long maxUtcMillis) {
        return prepareOffsetOrJavaTimeRounding(minUtcMillis, maxUtcMillis).maybeUseArray(
            minUtcMillis,
            maxUtcMillis,
            PreparedRounding.DEFAULT_ARRAY_ROUNDING_MAX_THRESHOLD
        );
    }

    private TimeUnitPreparedRounding prepareOffsetOrJavaTimeRounding(long minUtcMillis, long maxUtcMillis) {
        long minLookup = minUtcMillis - unit.extraLocalOffsetLookup();
        long maxLookup = maxUtcMillis;

        long unitMillis = 0;
        if (false == unitRoundsToMidnight) {
            /*
             * Units that round to midnight can round down from two
             * units worth of millis in the future to find the
             * nextRoundingValue.
             */
            unitMillis = unit.getField().getBaseUnit().getDuration().toMillis();
            maxLookup += 2 * unitMillis;
        }
        LocalTimeOffset.Lookup lookup = LocalTimeOffset.lookup(timeZone, minLookup, maxLookup);
        if (lookup == null) {
            // Range too long, just use java.time
            return prepareJavaTime();
        }
        LocalTimeOffset fixedOffset = lookup.fixedInRange(minLookup, maxLookup);
        if (fixedOffset != null) {
            // The time zone is effectively fixed
            if (unitRoundsToMidnight) {
                return new FixedToMidnightRounding(fixedOffset);
            }
            return new FixedNotToMidnightRounding(fixedOffset, unitMillis);
        }

        if (unitRoundsToMidnight) {
            return new ToMidnightRounding(lookup);
        }
        return new NotToMidnightRounding(lookup, unitMillis);
    }

    @Override
    public Prepared prepareForUnknown() {
        LocalTimeOffset offset = LocalTimeOffset.fixedOffset(timeZone);
        if (offset != null) {
            if (unitRoundsToMidnight) {
                return new FixedToMidnightRounding(offset);
            }
            return new FixedNotToMidnightRounding(offset, unit.getField().getBaseUnit().getDuration().toMillis());
        }
        return prepareJavaTime();
    }

    @Override
    TimeUnitPreparedRounding prepareJavaTime() {
        if (unitRoundsToMidnight) {
            return new JavaTimeToMidnightRounding();
        }
        return new JavaTimeNotToMidnightRounding(unit.getField().getBaseUnit().getDuration().toMillis());
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
        return Objects.hash(unit, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        org.opensearch.common.time.TimeUnitRounding other = (org.opensearch.common.time.TimeUnitRounding) obj;
        return Objects.equals(unit, other.unit) && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String toString() {
        return "Rounding[" + unit + " in " + timeZone + "]";
    }

    private abstract class TimeUnitPreparedRounding extends PreparedRounding {
        @Override
        public double roundingSize(long utcMillis, DateTimeUnit timeUnit) {
            if (timeUnit.isMillisBased() == unit.isMillisBased()) {
                return (double) unit.ratio / timeUnit.ratio;
            } else {
                if (unit.isMillisBased() == false) {
                    return (double) (nextRoundingValue(utcMillis) - utcMillis) / timeUnit.ratio;
                } else {
                    throw new IllegalArgumentException(
                        "Cannot use month-based rate unit ["
                            + timeUnit.shortName()
                            + "] with non-month based calendar interval histogram ["
                            + unit.shortName()
                            + "] only week, day, hour, minute and second are supported for this histogram"
                    );
                }
            }
        }
    }

    private class FixedToMidnightRounding extends TimeUnitPreparedRounding {
        private final LocalTimeOffset offset;

        FixedToMidnightRounding(LocalTimeOffset offset) {
            this.offset = offset;
        }

        @Override
        public long round(long utcMillis) {
            return offset.localToUtcInThisOffset(unit.roundFloor(offset.utcToLocalTime(utcMillis)));
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            // TODO this is used in date range's collect so we should optimize it too
            return new JavaTimeToMidnightRounding().nextRoundingValue(utcMillis);
        }
    }

    private class FixedNotToMidnightRounding extends TimeUnitPreparedRounding {
        private final LocalTimeOffset offset;
        private final long unitMillis;

        FixedNotToMidnightRounding(LocalTimeOffset offset, long unitMillis) {
            this.offset = offset;
            this.unitMillis = unitMillis;
        }

        @Override
        public long round(long utcMillis) {
            return offset.localToUtcInThisOffset(unit.roundFloor(offset.utcToLocalTime(utcMillis)));
        }

        @Override
        public final long nextRoundingValue(long utcMillis) {
            return round(utcMillis + unitMillis);
        }
    }

    private class ToMidnightRounding extends TimeUnitPreparedRounding implements LocalTimeOffset.Strategy {
        private final LocalTimeOffset.Lookup lookup;

        ToMidnightRounding(LocalTimeOffset.Lookup lookup) {
            this.lookup = lookup;
        }

        @Override
        public long round(long utcMillis) {
            LocalTimeOffset offset = lookup.lookup(utcMillis);
            return offset.localToUtc(unit.roundFloor(offset.utcToLocalTime(utcMillis)), this);
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            // TODO this is actually used date range's collect so we should optimize it
            return new JavaTimeToMidnightRounding().nextRoundingValue(utcMillis);
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
            return overlap.previous().localToUtc(localMillis, this);
        }

        @Override
        public long beforeOverlap(long localMillis, LocalTimeOffset.Overlap overlap) {
            return overlap.previous().localToUtc(localMillis, this);
        }

        @Override
        protected Prepared maybeUseArray(long minUtcMillis, long maxUtcMillis, int max) {
            if (lookup.anyMoveBackToPreviousDay()) {
                return this;
            }
            return super.maybeUseArray(minUtcMillis, maxUtcMillis, max);
        }
    }

    private class NotToMidnightRounding extends AbstractNotToMidnightRounding implements LocalTimeOffset.Strategy {
        private final LocalTimeOffset.Lookup lookup;

        NotToMidnightRounding(LocalTimeOffset.Lookup lookup, long unitMillis) {
            super(unitMillis);
            this.lookup = lookup;
        }

        @Override
        public long round(long utcMillis) {
            LocalTimeOffset offset = lookup.lookup(utcMillis);
            long roundedLocalMillis = unit.roundFloor(offset.utcToLocalTime(utcMillis));
            return offset.localToUtc(roundedLocalMillis, this);
        }

        @Override
        public long inGap(long localMillis, LocalTimeOffset.Gap gap) {
            // Round from just before the start of the gap
            return gap.previous().localToUtc(unit.roundFloor(gap.firstMissingLocalTime() - 1), this);
        }

        @Override
        public long beforeGap(long localMillis, LocalTimeOffset.Gap gap) {
            return inGap(localMillis, gap);
        }

        @Override
        public long inOverlap(long localMillis, LocalTimeOffset.Overlap overlap) {
            // Convert the overlap at this offset because that'll produce the largest result.
            return overlap.localToUtcInThisOffset(localMillis);
        }

        @Override
        public long beforeOverlap(long localMillis, LocalTimeOffset.Overlap overlap) {
            if (overlap.firstNonOverlappingLocalTime() - overlap.firstOverlappingLocalTime() >= unitMillis) {
                return overlap.localToUtcInThisOffset(localMillis);
            }
            return overlap.previous().localToUtc(localMillis, this); // This is mostly for Asia/Lord_Howe
        }
    }

    private class JavaTimeToMidnightRounding extends TimeUnitPreparedRounding {
        @Override
        public long round(long utcMillis) {
            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), timeZone);
            LocalDateTime localMidnight = truncateLocalDateTime(localDateTime);
            return firstTimeOnDay(localMidnight);
        }

        @Override
        public long nextRoundingValue(long utcMillis) {
            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), timeZone);
            LocalDateTime earlierLocalMidnight = truncateLocalDateTime(localDateTime);
            LocalDateTime localMidnight = nextRelevantMidnight(earlierLocalMidnight);
            return firstTimeOnDay(localMidnight);
        }

        @Override
        protected Prepared maybeUseArray(long minUtcMillis, long maxUtcMillis, int max) {
            // We don't have the right information needed to know if this is safe for this time zone so we always use java rounding
            return this;
        }

        private long firstTimeOnDay(LocalDateTime localMidnight) {
            assert localMidnight.toLocalTime().equals(LocalTime.of(0, 0, 0)) : "firstTimeOnDay should only be called at midnight";

            // Now work out what localMidnight actually means
            final List<ZoneOffset> currentOffsets = timeZone.getRules().getValidOffsets(localMidnight);
            if (currentOffsets.isEmpty() == false) {
                // There is at least one midnight on this day, so choose the first
                final ZoneOffset firstOffset = currentOffsets.get(0);
                final OffsetDateTime offsetMidnight = localMidnight.atOffset(firstOffset);
                return offsetMidnight.toInstant().toEpochMilli();
            } else {
                // There were no midnights on this day, so we must have entered the day via an offset transition.
                // Use the time of the transition as it is the earliest time on the right day.
                ZoneOffsetTransition zoneOffsetTransition = timeZone.getRules().getTransition(localMidnight);
                return zoneOffsetTransition.getInstant().toEpochMilli();
            }
        }

        private LocalDateTime nextRelevantMidnight(LocalDateTime localMidnight) {
            assert localMidnight.toLocalTime().equals(LocalTime.MIDNIGHT) : "nextRelevantMidnight should only be called at midnight";

            switch (unit) {
                case DAY_OF_MONTH:
                    return localMidnight.plus(1, ChronoUnit.DAYS);
                case WEEK_OF_WEEKYEAR:
                    return localMidnight.plus(7, ChronoUnit.DAYS);
                case MONTH_OF_YEAR:
                    return localMidnight.plus(1, ChronoUnit.MONTHS);
                case QUARTER_OF_YEAR:
                    return localMidnight.plus(3, ChronoUnit.MONTHS);
                case YEAR_OF_CENTURY:
                    return localMidnight.plus(1, ChronoUnit.YEARS);
                default:
                    throw new IllegalArgumentException("Unknown round-to-midnight unit: " + unit);
            }
        }
    }

    private class JavaTimeNotToMidnightRounding extends AbstractNotToMidnightRounding {
        JavaTimeNotToMidnightRounding(long unitMillis) {
            super(unitMillis);
        }

        @Override
        public long round(long utcMillis) {
            Instant instant = Instant.ofEpochMilli(utcMillis);
            final ZoneRules rules = timeZone.getRules();
            while (true) {
                final Instant truncatedTime = truncateAsLocalTime(instant, rules);
                final ZoneOffsetTransition previousTransition = rules.previousTransition(instant);

                if (previousTransition == null) {
                    // truncateAsLocalTime cannot have failed if there were no previous transitions
                    return truncatedTime.toEpochMilli();
                }

                Instant previousTransitionInstant = previousTransition.getInstant();
                if (truncatedTime != null && previousTransitionInstant.compareTo(truncatedTime) < 1) {
                    return truncatedTime.toEpochMilli();
                }

                // There was a transition in between the input time and the truncated time. Return to the transition time and
                // round that down instead.
                instant = previousTransitionInstant.minusNanos(1_000_000);
            }
        }

        private Instant truncateAsLocalTime(Instant instant, final ZoneRules rules) {
            assert unitRoundsToMidnight == false : "truncateAsLocalTime should not be called if unitRoundsToMidnight";

            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, timeZone);
            final LocalDateTime truncatedLocalDateTime = truncateLocalDateTime(localDateTime);
            final List<ZoneOffset> currentOffsets = rules.getValidOffsets(truncatedLocalDateTime);

            if (currentOffsets.isEmpty() == false) {
                // at least one possibilities - choose the latest one that's still no later than the input time
                for (int offsetIndex = currentOffsets.size() - 1; offsetIndex >= 0; offsetIndex--) {
                    final Instant result = truncatedLocalDateTime.atOffset(currentOffsets.get(offsetIndex)).toInstant();
                    if (result.isAfter(instant) == false) {
                        return result;
                    }
                }

                assert false : "rounded time not found for " + instant + " with " + this;
                return null;
            } else {
                // The chosen local time didn't happen. This means we were given a time in an hour (or a minute) whose start
                // is missing due to an offset transition, so the time cannot be truncated.
                return null;
            }
        }
    }

    private abstract class AbstractNotToMidnightRounding extends TimeUnitPreparedRounding {
        protected final long unitMillis;

        AbstractNotToMidnightRounding(long unitMillis) {
            this.unitMillis = unitMillis;
        }

        @Override
        public final long nextRoundingValue(long utcMillis) {
            final long roundedAfterOneIncrement = round(utcMillis + unitMillis);
            if (utcMillis < roundedAfterOneIncrement) {
                return roundedAfterOneIncrement;
            } else {
                return round(utcMillis + 2 * unitMillis);
            }
        }
    }
}
