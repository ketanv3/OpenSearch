/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import org.opensearch.OpenSearchException;
import org.opensearch.common.LocalTimeOffset;
import org.opensearch.common.annotation.InternalApi;

import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * A Date Time Unit
 *
 * @opensearch.internal
 */
@InternalApi
public enum DateTimeUnit {
    WEEK_OF_WEEKYEAR((byte) 1, "week", IsoFields.WEEK_OF_WEEK_BASED_YEAR, true, TimeUnit.DAYS.toMillis(7)) {
        private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(7);

        long roundFloor(long utcMillis) {
            return DateUtils.roundWeekOfWeekYear(utcMillis);
        }

        @Override
        long extraLocalOffsetLookup() {
            return extraLocalOffsetLookup;
        }
    },
    YEAR_OF_CENTURY((byte) 2, "year", ChronoField.YEAR_OF_ERA, false, 12) {
        private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(366);

        long roundFloor(long utcMillis) {
            return DateUtils.roundYear(utcMillis);
        }

        long extraLocalOffsetLookup() {
            return extraLocalOffsetLookup;
        }
    },
    QUARTER_OF_YEAR((byte) 3, "quarter", IsoFields.QUARTER_OF_YEAR, false, 3) {
        private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(92);

        long roundFloor(long utcMillis) {
            return DateUtils.roundQuarterOfYear(utcMillis);
        }

        long extraLocalOffsetLookup() {
            return extraLocalOffsetLookup;
        }
    },
    MONTH_OF_YEAR((byte) 4, "month", ChronoField.MONTH_OF_YEAR, false, 1) {
        private final long extraLocalOffsetLookup = TimeUnit.DAYS.toMillis(31);

        long roundFloor(long utcMillis) {
            return DateUtils.roundMonthOfYear(utcMillis);
        }

        long extraLocalOffsetLookup() {
            return extraLocalOffsetLookup;
        }
    },
    DAY_OF_MONTH((byte) 5, "day", ChronoField.DAY_OF_MONTH, true, ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis()) {
        long roundFloor(long utcMillis) {
            return DateUtils.roundFloor(utcMillis, this.ratio);
        }

        long extraLocalOffsetLookup() {
            return ratio;
        }
    },
    HOUR_OF_DAY((byte) 6, "hour", ChronoField.HOUR_OF_DAY, true, ChronoField.HOUR_OF_DAY.getBaseUnit().getDuration().toMillis()) {
        long roundFloor(long utcMillis) {
            return DateUtils.roundFloor(utcMillis, ratio);
        }

        long extraLocalOffsetLookup() {
            return ratio;
        }
    },
    MINUTES_OF_HOUR(
        (byte) 7,
        "minute",
        ChronoField.MINUTE_OF_HOUR,
        true,
        ChronoField.MINUTE_OF_HOUR.getBaseUnit().getDuration().toMillis()
    ) {
        long roundFloor(long utcMillis) {
            return DateUtils.roundFloor(utcMillis, ratio);
        }

        long extraLocalOffsetLookup() {
            return ratio;
        }
    },
    SECOND_OF_MINUTE(
        (byte) 8,
        "second",
        ChronoField.SECOND_OF_MINUTE,
        true,
        ChronoField.SECOND_OF_MINUTE.getBaseUnit().getDuration().toMillis()
    ) {
        long roundFloor(long utcMillis) {
            return DateUtils.roundFloor(utcMillis, ratio);
        }

        long extraLocalOffsetLookup() {
            return ratio;
        }
    };

    private final byte id;
    private final TemporalField field;
    private final boolean isMillisBased;
    private final String shortName;
    /**
     * ratio to milliseconds if isMillisBased == true or to month otherwise
     */
    protected final long ratio;

    DateTimeUnit(byte id, String shortName, TemporalField field, boolean isMillisBased, long ratio) {
        this.id = id;
        this.shortName = shortName;
        this.field = field;
        this.isMillisBased = isMillisBased;
        this.ratio = ratio;
    }

    /**
     * This rounds down the supplied milliseconds since the epoch down to the next unit. In order to retain performance this method
     * should be as fast as possible and not try to convert dates to java-time objects if possible
     *
     * @param utcMillis the milliseconds since the epoch
     * @return the rounded down milliseconds since the epoch
     */
    abstract long roundFloor(long utcMillis);

    /**
     * When looking up {@link LocalTimeOffset} go this many milliseconds
     * in the past from the minimum millis since epoch that we plan to
     * look up so that we can see transitions that we might have rounded
     * down beyond.
     */
    abstract long extraLocalOffsetLookup();

    public byte getId() {
        return id;
    }

    public TemporalField getField() {
        return field;
    }

    public static DateTimeUnit resolve(String name) {
        return DateTimeUnit.valueOf(name.toUpperCase(Locale.ROOT));
    }

    public String shortName() {
        return shortName;
    }

    public static DateTimeUnit resolve(byte id) {
        switch (id) {
            case 1:
                return WEEK_OF_WEEKYEAR;
            case 2:
                return YEAR_OF_CENTURY;
            case 3:
                return QUARTER_OF_YEAR;
            case 4:
                return MONTH_OF_YEAR;
            case 5:
                return DAY_OF_MONTH;
            case 6:
                return HOUR_OF_DAY;
            case 7:
                return MINUTES_OF_HOUR;
            case 8:
                return SECOND_OF_MINUTE;
            default:
                throw new OpenSearchException("Unknown date time unit id [" + id + "]");
        }
    }

    public boolean isMillisBased() {
        return isMillisBased;
    }
}
