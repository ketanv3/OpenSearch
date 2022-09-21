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
import org.opensearch.monitor.jvm.JvmStats;

import java.util.concurrent.TimeUnit;

public class SearchBackpressureSettings {
    private static final long heapSizeBytes = JvmStats.jvmStats().getMem().getHeapMax().getBytes();

    public interface Defaults {
        long INTERVAL = 1000;

        double CANCELLATION_RATIO = 0.1;
        double CANCELLATION_RATE = 3.0 / TimeUnit.SECONDS.toNanos(1);
        double CANCELLATION_BURST = 10.0;

        boolean ENABLED = true;
        boolean ENFORCED = true;

        int NODE_DURESS_NUM_CONSECUTIVE_BREACHES = 3;
        double NODE_DURESS_CPU_THRESHOLD = 0.9;
        double NODE_DURESS_HEAP_THRESHOLD = 0.7;

        double SEARCH_HEAP_USAGE_THRESHOLD = 0.05;
        double SEARCH_TASK_HEAP_USAGE_THRESHOLD = 0.005;
        double SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD = 2.0;

        long SEARCH_TASK_CPU_TIME_THRESHOLD = 15;
        long SEARCH_TASK_ELAPSED_TIME_THRESHOLD = 30000;
    }

    // Static settings
    private final TimeValue interval;
    public static final Setting<Long> SETTING_INTERVAL = Setting.longSetting(
        "search_backpressure.interval",
        Defaults.INTERVAL,
        1,
        Setting.Property.NodeScope
    );

    private final double cancellationRatio;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio",
        Defaults.CANCELLATION_RATIO,
        0.0,
        1.0,
        Setting.Property.NodeScope
    );

    private final double cancellationRate;
    public static final Setting<Double> SETTING_CANCELLATION_RATE = Setting.doubleSetting(
        "search_backpressure.cancellation_rate",
        Defaults.CANCELLATION_RATE,
        0.0,
        Setting.Property.NodeScope
    );

    private final double cancellationBurst;
    public static final Setting<Double> SETTING_CANCELLATION_BURST = Setting.doubleSetting(
        "search_backpressure.cancellation_burst",
        Defaults.CANCELLATION_BURST,
        0.0,
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
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double nodeDuressCpuThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_CPU_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.cpu_threshold",
        Defaults.NODE_DURESS_CPU_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double nodeDuressHeapThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.heap_threshold",
        Defaults.NODE_DURESS_HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchHeapUsageThreshold;
    public static final Setting<Double> SETTING_SEARCH_HEAP_USAGE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_heap_usage_threshold",
        Defaults.SEARCH_HEAP_USAGE_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchTaskHeapUsageThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_USAGE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task_heap_usage_threshold",
        Defaults.SEARCH_TASK_HEAP_USAGE_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile double searchTaskHeapUsageVarianceThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task_heap_usage_variance",
        Defaults.SEARCH_TASK_HEAP_USAGE_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile long searchTaskCpuTimeThreshold;
    public static final Setting<Long> SETTING_SEARCH_TASK_CPU_TIME_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_task_cpu_time_threshold",
        Defaults.SEARCH_TASK_CPU_TIME_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile long searchTaskElapsedTimeThreshold;
    public static final Setting<Long> SETTING_SEARCH_TASK_ELAPSED_TIME_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_task_elapsed_time_threshold",
        Defaults.SEARCH_TASK_ELAPSED_TIME_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public SearchBackpressureSettings(Settings settings, ClusterSettings clusterSettings) {
        interval = new TimeValue(SETTING_INTERVAL.get(settings));
        cancellationRatio = SETTING_CANCELLATION_RATIO.get(settings);
        cancellationRate = SETTING_CANCELLATION_RATE.get(settings);
        cancellationBurst = SETTING_CANCELLATION_BURST.get(settings);

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

        searchTaskCpuTimeThreshold = SETTING_SEARCH_TASK_CPU_TIME_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_CPU_TIME_THRESHOLD, this::setSearchTaskCpuTimeThreshold);

        searchTaskElapsedTimeThreshold = SETTING_SEARCH_TASK_ELAPSED_TIME_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_ELAPSED_TIME_THRESHOLD, this::setSearchTaskElapsedTimeThreshold);
    }

    public TimeValue getInterval() {
        return interval;
    }

    public double getCancellationRatio() {
        return cancellationRatio;
    }

    public double getCancellationRate() {
        return cancellationRate;
    }

    public double getCancellationBurst() {
        return cancellationBurst;
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

    public long getSearchHeapUsageThresholdBytes() {
        return (long) (heapSizeBytes * getSearchHeapUsageThreshold());
    }

    public void setSearchHeapUsageThreshold(double searchHeapUsageThreshold) {
        this.searchHeapUsageThreshold = searchHeapUsageThreshold;
    }

    public double getSearchTaskHeapUsageThreshold() {
        return searchTaskHeapUsageThreshold;
    }

    public long getSearchTaskHeapUsageThresholdBytes() {
        return (long) (heapSizeBytes * getSearchTaskHeapUsageThreshold());
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

    public long getSearchTaskCpuTimeThreshold() {
        return searchTaskCpuTimeThreshold;
    }

    public void setSearchTaskCpuTimeThreshold(long searchTaskCpuTimeThreshold) {
        this.searchTaskCpuTimeThreshold = searchTaskCpuTimeThreshold;
    }

    public long getSearchTaskElapsedTimeThreshold() {
        return searchTaskElapsedTimeThreshold;
    }

    public void setSearchTaskElapsedTimeThreshold(long searchTaskElapsedTimeThreshold) {
        this.searchTaskElapsedTimeThreshold = searchTaskElapsedTimeThreshold;
    }
}
