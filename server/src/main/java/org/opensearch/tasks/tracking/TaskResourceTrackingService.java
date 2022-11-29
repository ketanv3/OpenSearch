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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.ResourceStats;
import org.opensearch.tasks.ResourceStatsType;
import org.opensearch.tasks.ResourceUsageMetric;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskResourceTrackingService implements TaskAwareRunnable.Listener {
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

        onTaskStarted(task);
        onRunnableStarted(task, Thread.currentThread().getId());

        return () -> {
            onRunnableCompleted(task, Thread.currentThread().getId());
            storedContext.restore();
        };
    }

    public void onTaskStarted(Task task) {
        // Setting the initial count to one to account for the thread that actually started this task.
        runnableCount.put(task, new AtomicInteger(1));
    }

    public void onTaskCompleted(Task task) {
        runnableCount.remove(task);
        logger.info("task completed: {} {} {}", task.getAction(), task.getId(), task.getTotalResourceStats());
    }

    @Override
    public void onRunnableSubmitted(Task task, long threadId) {
        validate(task);
        runnableCount.get(task).incrementAndGet();
    }

    @Override
    public void onRunnableStarted(Task task, long threadId) {
        validate(task);
        task.startThreadResourceTracking(
            threadId,
            ResourceStatsType.WORKER_STATS,
            getThreadResourceUsageMetrics(threadId)
        );
    }

    @Override
    public void onRunnableCompleted(Task task, long threadId) {
        validate(task);
        int count = runnableCount.get(task).decrementAndGet();
        assert count >= 0 : "thread count cannot be negative";

        task.stopThreadResourceTracking(
            threadId,
            ResourceStatsType.WORKER_STATS,
            getThreadResourceUsageMetrics(threadId)
        );

        if (count == 0) {
            onTaskCompleted(task);
        }
    }

    private void validate(Task task) {
        assert runnableCount.containsKey(task) : "task must be present";
    }

    private ResourceUsageMetric[] getThreadResourceUsageMetrics(long threadId) {
        return new ResourceUsageMetric[] {
            new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId)),
            new ResourceUsageMetric(ResourceStats.MEMORY, threadMXBean.getThreadAllocatedBytes(threadId))
        };
    }
}
