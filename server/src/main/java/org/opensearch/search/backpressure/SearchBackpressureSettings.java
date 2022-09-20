/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

public class SearchBackpressureSettings {
    public interface Defaults {
        long INTERVAL = 1000; // in milliseconds

        boolean ENABLED = true;
        boolean ENFORCED = true;
    }

    // Static settings
    private final TimeValue interval;
    public static final Setting<Long> SETTING_INTERVAL = Setting.longSetting(
        "search_backpressure.interval",
        Defaults.INTERVAL,
        Setting.Property.NodeScope
    );

    // Dynamic settings
    private volatile boolean enabled;
    public static final Setting<Boolean> SETTING_ENABLED = Setting.boolSetting(
        "search_backpressure.enabled",
        Defaults.ENABLED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enforced;
    public static final Setting<Boolean> SETTING_ENFORCED = Setting.boolSetting(
        "search_backpressure.enforced",
        Defaults.ENFORCED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchBackpressureSettings(Settings settings, ClusterSettings clusterSettings) {
        interval = new TimeValue(SETTING_INTERVAL.get(settings));

        enabled = SETTING_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENABLED, this::setEnabled);

        enforced = SETTING_ENFORCED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENFORCED, this::setEnforced);
    }

    public TimeValue getInterval() {
        return interval;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnforced() {
        return enforced;
    }

    public void setEnforced(boolean enforced) {
        this.enforced = enforced;
    }
}
