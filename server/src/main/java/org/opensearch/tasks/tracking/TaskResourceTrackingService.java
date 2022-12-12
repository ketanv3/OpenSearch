/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.tracking;

import com.sun.management.ThreadMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.WrappedRunnable;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.ResourceStatsType;
import org.opensearch.tasks.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskResourceTrackingService implements OpenSearchThreadPoolExecutor.RunnableListener {
    private static final Logger logger = LogManager.getLogger(TaskResourceTrackingService.class);
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final ThreadPool threadPool;

    // Keeps track of tasks and number of pending runnable associated with it.
    private final Map<Task, AtomicInteger> runnableCount = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // TODO: Check if a {@link java.util.WeakHashMap} can be used to avoid task leaks.
    // It may be slower, but goes with the assumption that updates to this map are not very frequent.
    // A custom implementation which uses a readers-writer lock may perform even better.
    // private final Map<Task, AtomicInteger> runnableCount = Collections.synchronizedMap(new WeakHashMap<>());

    @Inject
    public TaskResourceTrackingService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public boolean isEnabled() {
        return true;
    }

    /**
     * Adds the given task to the thread context and starts resource tracking on the current thread. Forked threads will
     * preserve the thread context to be able to aggregate the resource usage for the same task.
     *
     * Returns an {@link AutoCloseable} which restores the thread context to its original state.
     */
    public ThreadContext.StoredContext setup(Task task) {
        if (isEnabled() == false || task.supportsResourceTracking() == false) {
            return () -> {};
        }

        ThreadContext context = threadPool.getThreadContext();
        ThreadContext.StoredContext storedContext = context.newStoredContext(true, Collections.singletonList(Task.THREAD_CONTEXT_TASK));
        context.putTransient(Task.THREAD_CONTEXT_TASK, task);

        TaskAwareRunnable marker = new TaskAwareRunnable(context, () -> {});
        onTaskStarted(task);
        onRunnableStart(marker);

        return () -> {
            onRunnableComplete(marker);
            storedContext.restore();
        };
    }

    public void onTaskStarted(Task task) {
        // Setting the initial count to one to account for the thread that actually started this task.
        runnableCount.put(task, new AtomicInteger(1));
        log("task start", task, null);
    }

    public void onTaskCompleted(Task task) {
        runnableCount.remove(task);
        log("task complete", task, null);
    }

    @Override
    public void onRunnableSubmit(Runnable runnable) {
        Task task = getTask(runnable);
        log("runnable submit", task, runnable);
        if (task == null) {
            return;
        }

        validate(task);
        runnableCount.get(task).incrementAndGet();
    }

    @Override
    public void onRunnableStart(Runnable runnable) {
        Task task = getTask(runnable);
        log("runnable start", task, runnable);
        if (task == null) {
            return;
        }

        validate(task);
        task.startThreadResourceTracking(
            Thread.currentThread().getId(),
            ResourceStatsType.WORKER_STATS,
            getThreadResourceUsageMetrics(Thread.currentThread().getId())
        );
    }

    @Override
    public void onRunnableComplete(Runnable runnable) {
        Task task = getTask(runnable);
        log("runnable complete", task, runnable);
        if (task == null) {
            return;
        }

        validate(task);
        int count = runnableCount.get(task).decrementAndGet();
        assert count >= 0 : "thread count cannot be negative";

        task.stopThreadResourceTracking(
            Thread.currentThread().getId(),
            ResourceStatsType.WORKER_STATS,
            getThreadResourceUsageMetrics(Thread.currentThread().getId())
        );

        if (count == 0) {
            onTaskCompleted(task);
        }
    }

    @Nullable
    private Task getTask(Runnable runnable) {
        Runnable original = runnable;

        while (runnable instanceof WrappedRunnable) {
            if (runnable instanceof TaskAwareRunnable) {
                log("found runnable", null, ((TaskAwareRunnable) runnable).unwrap());
                return ((TaskAwareRunnable) runnable).getTask();
            }

            runnable = ((WrappedRunnable) runnable).unwrap();
        }

        return null;
    }

    private void validate(Task task) {
        assert runnableCount.containsKey(task) : "task must be present";
    }

    private void log(String name, Task task, Runnable runnable) {
        logger.info("thread={} msg={} task={} runnable={}", Thread.currentThread().getId(), name, task, runnable);
    }

    private static ResourceUsageMetric[] getThreadResourceUsageMetrics(long threadId) {
        return new ResourceUsageMetric[] {
            new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId)),
            new ResourceUsageMetric(ResourceStats.MEMORY, threadMXBean.getThreadAllocatedBytes(threadId))
        };
    }
}
