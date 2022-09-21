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
        double CANCELLATION_RATE = 0.003;
        double CANCELLATION_BURST = 10.0;

        boolean ENABLED = true;
        boolean ENFORCED = true;

        int NODE_DURESS_NUM_CONSECUTIVE_BREACHES = 3;
        double NODE_DURESS_CPU_THRESHOLD = 0.9;
        double NODE_DURESS_HEAP_THRESHOLD = 0.7;

        double SEARCH_HEAP_THRESHOLD = 0.05;
        double SEARCH_TASK_HEAP_THRESHOLD = 0.005;
        double SEARCH_TASK_HEAP_VARIANCE_THRESHOLD = 2.0;

        long SEARCH_TASK_CPU_TIME_THRESHOLD = 15;
        long SEARCH_TASK_ELAPSED_TIME_THRESHOLD = 30000;
    }

    // Static settings

    /**
     * Defines the interval (in millis) at which the SearchBackpressureManager monitors and cancels tasks.
     */
    private final TimeValue interval;
    public static final Setting<Long> SETTING_INTERVAL = Setting.longSetting(
        "search_backpressure.interval",
        Defaults.INTERVAL,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Defines the percentage of tasks to cancel relative to the number of successful task completions.
     * In other words, it is the number of tokens added to the bucket on each successful task completion.
     */
    private final double cancellationRatio;
    public static final Setting<Double> SETTING_CANCELLATION_RATIO = Setting.doubleSetting(
        "search_backpressure.cancellation_ratio",
        Defaults.CANCELLATION_RATIO,
        0.0,
        1.0,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of tasks to cancel per unit time (in millis).
     * In other words, it is the number of tokens added to the bucket each millisecond.
     */
    private final double cancellationRate;
    public static final Setting<Double> SETTING_CANCELLATION_RATE = Setting.doubleSetting(
        "search_backpressure.cancellation_rate",
        Defaults.CANCELLATION_RATE,
        0.0,
        Setting.Property.NodeScope
    );

    /**
     * Defines the maximum number of tasks that can be cancelled before being rate-limited.
     */
    private final double cancellationBurst;
    public static final Setting<Double> SETTING_CANCELLATION_BURST = Setting.doubleSetting(
        "search_backpressure.cancellation_burst",
        Defaults.CANCELLATION_BURST,
        1.0,
        Setting.Property.NodeScope
    );

    // Dynamic settings

    /**
     * Defines whether search backpressure is enabled or not.
     */
    private volatile boolean enabled;
    public static final Setting<Boolean> SETTING_ENABLED = Setting.boolSetting(
        "search_backpressure.enabled",
        Defaults.ENABLED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines whether in-flight cancellation of tasks is enabled or not.
     */
    private volatile boolean enforced;
    public static final Setting<Boolean> SETTING_ENFORCED = Setting.boolSetting(
        "search_backpressure.enforced",
        Defaults.ENFORCED,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the number of consecutive limit breaches after the node is marked "in duress".
     */
    private volatile int nodeDuressNumConsecutiveBreaches;
    public static final Setting<Integer> SETTING_NODE_DURESS_NUM_CONSECUTIVE_BREACHES = Setting.intSetting(
        "search_backpressure.node_duress.num_consecutive_breaches",
        Defaults.NODE_DURESS_NUM_CONSECUTIVE_BREACHES,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in percentage) for a node to be considered "in duress".
     */
    private volatile double nodeDuressCpuThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_CPU_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.cpu_threshold",
        Defaults.NODE_DURESS_CPU_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for a node to be considered "in duress".
     */
    private volatile double nodeDuressHeapThreshold;
    public static final Setting<Double> SETTING_NODE_DURESS_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.heap_threshold",
        Defaults.NODE_DURESS_HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for the sum of heap usages across all search tasks
     * before in-flight cancellation is applied.
     */
    private volatile double searchHeapThreshold;
    public static final Setting<Double> SETTING_SEARCH_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_heap_threshold",
        Defaults.SEARCH_HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for an individual task before it is considered for cancellation.
     */
    private volatile double searchTaskHeapThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task_heap_threshold",
        Defaults.SEARCH_TASK_HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage variance for an individual task before it is considered for cancellation.
     * A task is considered for cancellation when (taskHeapUsage >= heapUsageMovingAverage * variance).
     */
    private volatile double searchTaskHeapVarianceThreshold;
    public static final Setting<Double> SETTING_SEARCH_TASK_HEAP_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.search_task_heap_variance",
        Defaults.SEARCH_TASK_HEAP_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long searchTaskCpuTimeThreshold;
    public static final Setting<Long> SETTING_SEARCH_TASK_CPU_TIME_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_task_cpu_time_threshold",
        Defaults.SEARCH_TASK_CPU_TIME_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the elapsed time threshold (in millis) for an individual task before it is considered for cancellation.
     */
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

        searchHeapThreshold = SETTING_SEARCH_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_HEAP_THRESHOLD, this::setSearchHeapThreshold);

        searchTaskHeapThreshold = SETTING_SEARCH_TASK_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_HEAP_THRESHOLD, this::setSearchTaskHeapThreshold);

        searchTaskHeapVarianceThreshold = SETTING_SEARCH_TASK_HEAP_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_SEARCH_TASK_HEAP_VARIANCE_THRESHOLD, this::setSearchTaskHeapVarianceThreshold);

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

    public double getCancellationRateNanos() {
        return getCancellationRate() / TimeUnit.MILLISECONDS.toNanos(1); // rate per nanoseconds
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

    public double getSearchHeapThreshold() {
        return searchHeapThreshold;
    }

    public long getSearchHeapThresholdBytes() {
        return (long) (heapSizeBytes * getSearchHeapThreshold());
    }

    public void setSearchHeapThreshold(double searchHeapThreshold) {
        this.searchHeapThreshold = searchHeapThreshold;
    }

    public double getSearchTaskHeapThreshold() {
        return searchTaskHeapThreshold;
    }

    public long getSearchTaskHeapThresholdBytes() {
        return (long) (heapSizeBytes * getSearchTaskHeapThreshold());
    }

    public void setSearchTaskHeapThreshold(double searchTaskHeapThreshold) {
        this.searchTaskHeapThreshold = searchTaskHeapThreshold;
    }

    public double getSearchTaskHeapVarianceThreshold() {
        return searchTaskHeapVarianceThreshold;
    }

    public void setSearchTaskHeapVarianceThreshold(double searchTaskHeapVarianceThreshold) {
        this.searchTaskHeapVarianceThreshold = searchTaskHeapVarianceThreshold;
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
