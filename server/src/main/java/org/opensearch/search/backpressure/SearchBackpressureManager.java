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
import org.opensearch.common.Streak;
import org.opensearch.common.util.TokenBucket;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.search.backpressure.stats.CancellationStats;
import org.opensearch.search.backpressure.stats.CancelledTaskStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResourceTrackingService.TaskCompletionListener;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class SearchBackpressureManager implements Runnable, TaskCompletionListener {
    private static final Logger logger = LogManager.getLogger(SearchBackpressureManager.class);
    private static final OperatingSystemMXBean osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private final SearchBackpressureSettings settings;
    private final TaskResourceTrackingService taskResourceTrackingService;
    private final List<ResourceUsageTracker> trackers;

    private final Streak cpuBreachesStreak = new Streak();
    private final Streak heapBreachesStreak = new Streak();

    private final AtomicLong completionCount = new AtomicLong();
    private final AtomicLong cancellationCount = new AtomicLong();
    private final AtomicLong limitReachedCount = new AtomicLong();
    private final AtomicReference<CancelledTaskStats> lastCancelledTaskUsage = new AtomicReference<>();

    private final TokenBucket rateLimitPerTime;
    private final TokenBucket rateLimitPerTaskCompletion;

    private final LongSupplier timeNanosSupplier;
    private final DoubleSupplier cpuUsageSupplier;
    private final DoubleSupplier heapUsageSupplier;

    public SearchBackpressureManager(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool
    ) {
        this(
            settings,
            taskResourceTrackingService,
            threadPool,
            System::nanoTime,
            osMXBean::getProcessCpuLoad,
            () -> JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100.0,
            List.of(
                new CpuUsageTracker(() -> TimeUnit.MILLISECONDS.toNanos(settings.getSearchTaskCpuTimeThreshold())),
                new HeapUsageTracker(
                    settings::getSearchTaskHeapUsageThresholdBytes,
                    settings::getSearchTaskHeapUsageVarianceThreshold
                ),
                new ElapsedTimeTracker(System::nanoTime, () -> TimeUnit.MILLISECONDS.toNanos(settings.getSearchTaskElapsedTimeThreshold()))
            )
        );
    }

    public SearchBackpressureManager(
        SearchBackpressureSettings settings,
        TaskResourceTrackingService taskResourceTrackingService,
        ThreadPool threadPool,
        LongSupplier timeNanosSupplier,
        DoubleSupplier cpuUsageSupplier,
        DoubleSupplier heapUsageSupplier,
        List<ResourceUsageTracker> trackers
    ) {
        this.settings = settings;
        this.taskResourceTrackingService = taskResourceTrackingService;
        this.taskResourceTrackingService.addTaskCompletionListener(this);
        this.trackers = trackers;
        this.rateLimitPerTime = new TokenBucket(timeNanosSupplier, getSettings().getCancellationRate(), getSettings().getCancellationBurst());
        this.rateLimitPerTaskCompletion = new TokenBucket(completionCount::get, getSettings().getCancellationRatio(), getSettings().getCancellationBurst());
        this.timeNanosSupplier = timeNanosSupplier;
        this.cpuUsageSupplier = cpuUsageSupplier;
        this.heapUsageSupplier = heapUsageSupplier;

        threadPool.scheduleWithFixedDelay(this, getSettings().getInterval(), ThreadPool.Names.SAME);
    }

    @Override
    public void run() {
        if (getSettings().isEnabled() == false) {
            return;
        }

        if (isNodeInDuress() == false) {
            return;
        }

        // We are only targeting in-flight cancellation of SearchShardTask for now.
        List<CancellableTask> searchShardTasks = getSearchShardTasks();

        // Force-refresh usage stats of these tasks before making a cancellation decision.
        taskResourceTrackingService.refreshResourceStats(searchShardTasks.toArray(new Task[0]));

        // Skip cancellation if the increase in heap usage is not due to search requests.
        long runningTasksHeapUsage = searchShardTasks.stream().mapToLong(task -> task.getTotalResourceStats().getMemoryInBytes()).sum();
        if (runningTasksHeapUsage < getSettings().getSearchHeapUsageThresholdBytes()) {
            return;
        }

        for (TaskCancellation taskCancellation : getTaskCancellations(searchShardTasks)) {
            logger.info("cancelling task due to high resource consumption: id={} reason={}", taskCancellation.getTask().getId(), taskCancellation.getReasonString());

            if (getSettings().isEnforced() == false) {
                continue;
            }

            // Independently remove tokens from both token buckets.
            boolean timeRateLimitReached = rateLimitPerTime.request() == false;
            boolean taskCompletionRateLimitReached = rateLimitPerTaskCompletion.request() == false;

            // Stop cancelling tasks if there are no tokens in either of the two token buckets.
            if (timeRateLimitReached && taskCompletionRateLimitReached) {
                limitReachedCount.incrementAndGet();
                break;
            }

            CancelledTaskStats stats = taskCancellation.cancel();
            lastCancelledTaskUsage.set(stats);
            cancellationCount.incrementAndGet();
        }
    }

    /**
     * Returns true if the node is currently under duress.
     * + CPU usage is greater than 70% continuously for 3 observations
     * + Heap usage is greater than 80% continuously for 3 observations
     */
    boolean isNodeInDuress() {
        logger.info("cpu={} mem={}", cpuUsageSupplier.getAsDouble(), heapUsageSupplier.getAsDouble());

        boolean isCpuBreached = cpuUsageSupplier.getAsDouble() >= getSettings().getNodeDuressCpuThreshold();
        boolean isHeapBreached = heapUsageSupplier.getAsDouble() >= getSettings().getNodeDuressHeapThreshold();

        int numConsecutiveBreaches = getSettings().getNodeDuressNumConsecutiveBreaches();
        boolean isCpuConsecutivelyBreached = cpuBreachesStreak.record(isCpuBreached) >= numConsecutiveBreaches;
        boolean isHeapConsecutivelyBreached = heapBreachesStreak.record(isHeapBreached) >= numConsecutiveBreaches;

        return isCpuConsecutivelyBreached || isHeapConsecutivelyBreached;
    }

    /**
     * Filters and returns the list of actively running SearchShardTasks.
     */
    List<CancellableTask> getSearchShardTasks() {
        return taskResourceTrackingService.getResourceAwareTasks().values().stream()
            .filter(task -> task instanceof SearchShardTask)
            .map(task -> (CancellableTask) task)
            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Returns the TaskCancellation wrapper for the given task.
     * The TaskCancellation contains a list of reasons (possibly zero) to cancel the task, along with an overall
     * cancellation score. Cancelling a task with a higher score has a better chance of recovering the node from duress.
     */
    TaskCancellation getTaskCancellation(CancellableTask task) {
        List<TaskCancellation.Reason> reasons = trackers.stream()
            .map(tracker -> tracker.cancellationReason(task))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toUnmodifiableList());

        return new TaskCancellation(task, reasons, timeNanosSupplier);
    }

    /**
     * Returns the list of TaskCancellations sorted by descending order of their cancellation score.
     */
    List<TaskCancellation> getTaskCancellations(List<CancellableTask> tasks) {
        return tasks.stream()
            .map(this::getTaskCancellation)
            .filter(TaskCancellation::isEligibleForCancellation)
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (task instanceof SearchShardTask == false) {
            return;
        }

        SearchShardTask searchShardTask = (SearchShardTask) task;
        if (searchShardTask.isCancelled() == false) {
            completionCount.incrementAndGet();
        }

        for (ResourceUsageTracker tracker : trackers) {
            tracker.update(searchShardTask);
        }
    }

    public SearchBackpressureSettings getSettings() {
        return settings;
    }

    public long getCompletionCount() {
        return completionCount.get();
    }

    public long getCancellationCount() {
        return cancellationCount.get();
    }

    public long getLimitReachedCount() {
        return limitReachedCount.get();
    }

    public CancelledTaskStats getLastCancelledTaskUsage() {
        return lastCancelledTaskUsage.get();
    }

    /**
     * Returns the stats for the "/_node/stats/search_backpressure" API.
     */
    public SearchBackpressureStats nodeStats() {
        List<Task> searchShardTasks = taskResourceTrackingService.getResourceAwareTasks().values().stream()
            .filter(task -> task instanceof SearchShardTask)
            .collect(Collectors.toUnmodifiableList());

        Map<String, ResourceUsageTracker.Stats> currentStats = trackers.stream()
            .collect(Collectors.toMap(ResourceUsageTracker::name, tracker -> tracker.currentStats(searchShardTasks)));

        Map<String, Long> cancellationsBreakup = trackers.stream()
            .collect(Collectors.toMap(ResourceUsageTracker::name, ResourceUsageTracker::getCancellations));

        return new SearchBackpressureStats(
            currentStats,
            new CancellationStats(
                cancellationCount.get(),
                cancellationsBreakup,
                limitReachedCount.get(),
                lastCancelledTaskUsage.get()
            ),
            getSettings().isEnabled(),
            getSettings().isEnforced()
        );
    }
}
