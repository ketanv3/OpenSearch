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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SearchBackpressureManager implements Runnable, TaskResourceTrackingService.TaskCompletionListener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureManager.class);
    private static final OperatingSystemMXBean osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private final TaskResourceTrackingService taskResourceTrackingService;
    private final List<ResourceUsageTracker> trackers;

    private final AtomicInteger consecutiveCpuBreaches = new AtomicInteger(0);
    private final AtomicInteger consecutiveHeapBreaches = new AtomicInteger(0);

    static class Settings {
        public static final boolean ENABLED = true;

        public static final int NUM_CONSECUTIVE_BREACHES = 3;
        public static final double CPU_USAGE_THRESHOLD = 0.9;
        public static final double HEAP_USAGE_THRESHOLD = 70;

        public static final long MIN_HEAP_USAGE_FOR_SEARCH_BACKPRESSURE = (long) (JvmStats.jvmStats().getMem().getHeapMax().getBytes() * 0.05);
        public static final long MAX_TASK_CANCELLATION_PERCENTAGE = 10;
    }

    @Inject
    public SearchBackpressureManager(TaskResourceTrackingService taskResourceTrackingService, ThreadPool threadPool) {
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.trackers = List.of(new CpuUsageTracker(), new HeapUsageTracker(), new ElapsedTimeTracker());

        if (Settings.ENABLED) {
            threadPool.scheduleWithFixedDelay(this, TimeValue.timeValueSeconds(1), ThreadPool.Names.SAME);
        }
    }

    /**
     * Returns true if the node is currently under duress.
     * + CPU usage is greater than 70% continuously for 3 observations
     * + Heap usage is greater than 80% continuously for 3 observations
     */
    private boolean isNodeInDuress() {
        boolean isCpuBreached = false, isHeapBreached = false;

        if (osMXBean.getProcessCpuLoad() > Settings.CPU_USAGE_THRESHOLD) {
            isCpuBreached = consecutiveCpuBreaches.incrementAndGet() >= Settings.NUM_CONSECUTIVE_BREACHES;
        } else {
            consecutiveCpuBreaches.set(0);
        }

        if (JvmStats.jvmStats().getMem().getHeapUsedPercent() > Settings.HEAP_USAGE_THRESHOLD) {
            isHeapBreached = consecutiveHeapBreaches.incrementAndGet() >= Settings.NUM_CONSECUTIVE_BREACHES;
        } else {
            consecutiveHeapBreaches.set(0);
        }

        return isCpuBreached || isHeapBreached;
    }

    @Override
    public void run() {
        if (isNodeInDuress() == false) {
            return;
        }

        // We are only targeting in-flight cancellation of SearchShardTask for now.
        List<Task> searchShardTasks = taskResourceTrackingService.getResourceAwareTasks().values().stream()
            .filter(task -> task instanceof SearchShardTask)
            .collect(Collectors.toUnmodifiableList());

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Skip cancellation if the increase in heap usage is not due to search requests.
        long runningTasksHeapUsage = searchShardTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        if (runningTasksHeapUsage < Settings.MIN_HEAP_USAGE_FOR_SEARCH_BACKPRESSURE) {
            return;
        }

        for (CancellableTask task : getTasksForCancellation(searchShardTasks, 3)) {
            logger.info("cancelling task due to high resource consumption: id={} action={}", task.getId(), task.getAction());
            task.cancel("resource consumption exceeded");
        }
    }

    /**
     * Returns the list of tasks eligible for cancellation.
     * Tasks are returned in reverse sorted order of their cancellation score. Cancelling a task with higher score has
     * better chance of recovering the node from duress.
     */
    private List<CancellableTask> getTasksForCancellation(List<Task> tasks, int maxTasksToCancel) {
        return tasks.stream()
            .filter(task -> task instanceof CancellableTask)
            .map(task -> (CancellableTask) task)
            .filter(task -> task.isCancelled() == false)
            .filter(task -> trackers.stream().anyMatch(tracker -> tracker.shouldCancel(task)))
            .limit(maxTasksToCancel)
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (task instanceof SearchShardTask == false) {
            return;
        }

        for (ResourceUsageTracker tracker : trackers) {
            tracker.update(task);
        }
    }
}
