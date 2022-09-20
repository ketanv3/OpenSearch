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

        int NODE_DURESS_NUM_CONSECUTIVE_BREACHES = 3;
        double NODE_DURESS_CPU_THRESHOLD = 0.9;
        double NODE_DURESS_HEAP_THRESHOLD = 0.7;

        double SEARCH_HEAP_USAGE_THRESHOLD = 0.05;
        double SEARCH_TASK_HEAP_USAGE_THRESHOLD = 0.005;
        double SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD = 2.0;
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

    private volatile int nodeDuressNumConsecutiveBreaches;
    public static final Setting<Integer> SETTING_NODE_DURESS_NUM_CONSECUTIVE_BREACHES = Setting.intSetting(
        "search_backpressure.node_duress.num_consecutive_breaches",
        Defaults.NODE_DURESS_NUM_CONSECUTIVE_BREACHES,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double nodeDuressCpuThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_CPU_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.cpu_threshold",
        Defaults.NODE_DURESS_CPU_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double nodeDuressHeapThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.heap_threshold",
        Defaults.NODE_DURESS_HEAP_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchHeapUsageThreshold;
    public static final Setting<Double> SETTING_SEARCH_HEAP_USAGE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.heap_usage.search_threshold",
        Defaults.SEARCH_HEAP_USAGE_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchTaskHeapUsageThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_USAGE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.heap_usage.search_task_threshold",
        Defaults.SEARCH_TASK_HEAP_USAGE_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchTaskHeapUsageVarianceThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.heap_usage.search_task_variance",
        Defaults.SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchBackpressureSettings(Settings settings, ClusterSettings clusterSettings) {
        interval = new TimeValue(SETTING_INTERVAL.get(settings));

        enabled = SETTING_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENABLED, this::setEnabled);

        enforced = SETTING_ENFORCED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_ENFORCED, this::setEnforced);

        nodeDuressNumConsecutiveBreaches = SETTING_NODE_DURESS_NUM_CONSECUTIVE_BREACHES.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_NODE_DURESS_NUM_CONSECUTIVE_BREACHES, this::setNodeDuressNumConsecutiveBreaches);

        nodeDuressCpuThreshold = SETTING_NODE_DURESS_CPU_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_NODE_DURESS_CPU_THRESHOLD, this::setNodeDuressCpuThreshold);

        nodeDuressHeapThreshold = SETTING_NODE_DURESS_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_NODE_DURESS_HEAP_THRESHOLD, this::setNodeDuressHeapThreshold);

        searchHeapUsageThreshold = SETTING_SEARCH_HEAP_USAGE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_HEAP_USAGE_THRESHOLD, this::setSearchHeapUsageThreshold);

        searchTaskHeapUsageThreshold = SETTING_SEARCH_TASK_HEAP_USAGE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_HEAP_USAGE_THRESHOLD, this::setSearchTaskHeapUsageThreshold);

        searchTaskHeapUsageVarianceThreshold = SETTING_SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD, this::setSearchTaskHeapUsageVarianceThreshold);
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

    public int getNodeDuressNumConsecutiveBreaches() {
        return nodeDuressNumConsecutiveBreaches;
    }

    public void setNodeDuressNumConsecutiveBreaches(int nodeDuressNumConsecutiveBreaches) {
        this.nodeDuressNumConsecutiveBreaches = nodeDuressNumConsecutiveBreaches;
    }

    public double getNodeDuressCpuThreshold() {
        return nodeDuressCpuThreshold;
    }

    public void setNodeDuressCpuThreshold(double nodeDuressCpuThreshold) {
        this.nodeDuressCpuThreshold = nodeDuressCpuThreshold;
    }

    public double getNodeDuressHeapThreshold() {
        return nodeDuressHeapThreshold;
    }

    public void setNodeDuressHeapThreshold(double nodeDuressHeapThreshold) {
        this.nodeDuressHeapThreshold = nodeDuressHeapThreshold;
    }

    public double getSearchHeapUsageThreshold() {
        return searchHeapUsageThreshold;
    }

    public void setSearchHeapUsageThreshold(double searchHeapUsageThreshold) {
        this.searchHeapUsageThreshold = searchHeapUsageThreshold;
    }

    public double getSearchTaskHeapUsageThreshold() {
        return searchTaskHeapUsageThreshold;
    }

    public void setSearchTaskHeapUsageThreshold(double searchTaskHeapUsageThreshold) {
        this.searchTaskHeapUsageThreshold = searchTaskHeapUsageThreshold;
    }

    public double getSearchTaskHeapUsageVarianceThreshold() {
        return searchTaskHeapUsageVarianceThreshold;
    }

    public void setSearchTaskHeapUsageVarianceThreshold(double searchTaskHeapUsageVarianceThreshold) {
        this.searchTaskHeapUsageVarianceThreshold = searchTaskHeapUsageVarianceThreshold;
    }
}
