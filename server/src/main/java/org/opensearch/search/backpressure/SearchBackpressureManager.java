/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import com.sun.management.OperatingSystemMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SearchBackpressureManager implements Runnable, TaskCompletionListener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureManager.class);
    private static final OperatingSystemMXBean osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private static final TimeValue interval = TimeValue.timeValueSeconds(1);

    public static final Setting<Boolean> SEARCH_BACKPRESSURE_ENABLED = Setting.boolSetting(
        "search_backpressure.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SEARCH_BACKPRESSURE_ENFORCED = Setting.boolSetting(
        "search_backpressure.enforced",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enabled;
    private volatile boolean enforced;

    private final TaskResourceTrackingService taskResourceTrackingService;
    private final List<ResourceUsageTracker> trackers;

    private final AtomicInteger consecutiveCpuBreaches = new AtomicInteger(0);
    private final AtomicInteger consecutiveHeapBreaches = new AtomicInteger(0);
    private final AtomicInteger currentIterationCompletedTasks = new AtomicInteger(0);
    private final AtomicLong totalCancellations = new AtomicLong(0);

    @Inject
    public SearchBackpressureManager(
        Settings settings,
        ClusterSettings clusterSettings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool
    ) {
        this.enabled = SEARCH_BACKPRESSURE_ENABLED.get(settings);
        this.enforced = SEARCH_BACKPRESSURE_ENFORCED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_BACKPRESSURE_ENABLED, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(SEARCH_BACKPRESSURE_ENFORCED, this::setEnforced);

        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.trackers = List.of(new CpuUsageTracker(), new HeapUsageTracker(), new ElapsedTimeTracker());

        threadPool.scheduleWithFixedDelay(this, interval, ThreadPool.Names.SAME);
    }

    @Override
    public void run() {
        if (isEnabled() == false) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        // We are only targeting in-flight cancellation of SearchShardTask for now.
        List<CancellableTask> searchShardTasks = taskResourceTrackingService.getResourceAwareTasks().values().stream()
            .filter(task -> task instanceof SearchShardTask)
            .map(task -> (CancellableTask) task)
            .collect(Collectors.toUnmodifiableList());

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Skip cancellation if the increase in heap usage is not due to search requests.
        long runningTasksHeapUsage = searchShardTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        if (runningTasksHeapUsage < Thresholds.SEARCH_HEAP_USAGE_THRESHOLD_BYTES) {
            return;
        }

        // Calculate the maximum number of tasks to cancel based on the successful task completion rate.
        int maxTasksToCancel = Math.min(
            Thresholds.MAX_TASK_CANCELLATION_COUNT,
            Math.max(1, (int) (currentIterationCompletedTasks.get() * Thresholds.MAX_TASK_CANCELLATION_PERCENTAGE))
        );

        for (CancellableTask task : getTasksForCancellation(searchShardTasks, maxTasksToCancel)) {
            logger.info("cancelling task due to high resource consumption: id={} action={}", task.getId(), task.getAction());
            if (isEnforced()) {
                task.cancel("resource consumption exceeded");
                totalCancellations.incrementAndGet();
            }
        }

        // Reset the completed task count to zero.
        currentIterationCompletedTasks.set(0);
    }

    /**
     * Returns true if the node is currently under duress.
     * + CPU usage is greater than 70% continuously for 3 observations
     * + Heap usage is greater than 80% continuously for 3 observations
     */
    private boolean isNodeInDuress() {
        boolean isCpuBreached = false, isHeapBreached = false;

        logger.info(
            "cpu={}/{} mem={}/{}",
            osMXBean.getProcessCpuLoad(), Thresholds.NODE_DURESS_CPU_USAGE_THRESHOLD,
            JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100., Thresholds.NODE_DURESS_HEAP_USAGE_THRESHOLD
        );

        if (osMXBean.getProcessCpuLoad() > Thresholds.NODE_DURESS_CPU_USAGE_THRESHOLD) {
            isCpuBreached = consecutiveCpuBreaches.incrementAndGet() >= Thresholds.NUM_CONSECUTIVE_BREACHES;
        } else {
            consecutiveCpuBreaches.set(0);
        }

        if (JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100. > Thresholds.NODE_DURESS_HEAP_USAGE_THRESHOLD) {
            isHeapBreached = consecutiveHeapBreaches.incrementAndGet() >= Thresholds.NUM_CONSECUTIVE_BREACHES;
        } else {
            consecutiveHeapBreaches.set(0);
        }

        return isCpuBreached || isHeapBreached;
    }

    /**
     * Returns the list of tasks eligible for cancellation.
     * Tasks are returned in reverse sorted order of their cancellation score. Cancelling a task with higher score has
     * better chance of recovering the node from duress.
     */
    private List<CancellableTask> getTasksForCancellation(List<CancellableTask> tasks, int maxTasksToCancel) {
        return tasks.stream()
            .filter(task -> task.isCancelled() == false)
            .map(this::totalCancellationScore)
            .filter(tuple -> tuple.v2() > 0)
            .sorted((a, b) -> Double.compare(b.v2(), a.v2()))
            .limit(maxTasksToCancel)
            .map(Tuple::v1)
            .collect(Collectors.toUnmodifiableList());
    }

    private Tuple<CancellableTask, Double> totalCancellationScore(CancellableTask task) {
        double score = trackers.stream().mapToDouble(tracker -> tracker.cancellationScore(task)).sum();
        return new Tuple<>(task, score);
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (task instanceof SearchShardTask == false) {
            return;
        }

        SearchShardTask searchShardTask = (SearchShardTask) task;
        if (searchShardTask.isCancelled() == false) {
            currentIterationCompletedTasks.incrementAndGet();
        }

        for (ResourceUsageTracker tracker : trackers) {
            tracker.update(searchShardTask);
        }
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

    public long getTotalCancellations() {
        return totalCancellations.get();
    }

    public SearchBackpressureStats nodeStats() {
        return new SearchBackpressureStats();
    }
}
