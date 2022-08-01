/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import com.sun.management.ThreadMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskResourceTrackingService implements TaskAwareRunnable.Listener, TaskResourceTrackingListener {
    private static final Logger logger = LogManager.getLogger(TaskResourceTrackingService.class);

    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    public static final Setting<Boolean> TASK_RESOURCE_TRACKING_ENABLED = Setting.boolSetting(
        "task_resource_tracking.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ConcurrentMapLong<Task> resourceAwareTasks;
    private final ThreadPool threadPool;
    private volatile boolean taskResourceTrackingEnabled;
    private final List<TaskResourceTrackingListener> listeners;

    @Inject
    public TaskResourceTrackingService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.resourceAwareTasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();
        this.threadPool = threadPool;

        this.taskResourceTrackingEnabled = TASK_RESOURCE_TRACKING_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_TRACKING_ENABLED, this::setTaskResourceTrackingEnabled);

        this.listeners = Collections.synchronizedList(new ArrayList<>());
        addTaskResourceTrackingListener(this);
    }

    public void addTaskResourceTrackingListener(TaskResourceTrackingListener listener) {
        this.listeners.add(listener);
    }

    public void setTaskResourceTrackingEnabled(boolean taskResourceTrackingEnabled) {
        this.taskResourceTrackingEnabled = taskResourceTrackingEnabled;
    }

    public boolean isTaskResourceTrackingEnabled() {
        return taskResourceTrackingEnabled;
    }

    public static boolean isTaskResourceTrackingSupported() {
        return threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    /**
     * Returns the instantaneous thread resource usage metrics.
     * It is an estimate of total resource usage for a given thread ever since the JVM started.
     *
     * To calculate the resource usage of a runnable, which is more meaningful, the growth between starting and ending
     * resource usage metrics of the thread must be taken.
     */
    private static ResourceUsageMetric[] getThreadResourceUsageMetrics(long threadId) {
        return new ResourceUsageMetric[] {
            new ResourceUsageMetric(ResourceStats.CPU, threadMXBean.getThreadCpuTime(threadId)),
            new ResourceUsageMetric(ResourceStats.MEMORY, threadMXBean.getThreadAllocatedBytes(threadId))
        };
    }

    /**
     * Starts resource tracking for the given task.
     * This adds the taskId to the threadContext which is preserved across other forked threads.
     *
     * When threads execute a {@link TaskAwareRunnable}, the {@link TaskAwareRunnable.Listener} (implemented here)
     * is invoked with the taskId and threadId.
     *  - onThreadExecutionStarted(long taskId, long threadId)
     *  - onThreadExecutionStopped(long taskId, long threadId)
     *
     * The Task object is obtained from the taskId, and {@link TaskResourceTrackingListener} (also implemented here)
     * is invoked subsequently with the Task object and threadId.
     *  - onThreadExecutionStarted(Task task, long threadId)
     *  - onThreadExecutionStopped(Task task, long threadId)
     */
    public ThreadContext.StoredContext startResourceTracking(Task task) {
        if (task.supportsResourceTracking() == false || isTaskResourceTrackingEnabled() == false || isTaskResourceTrackingSupported() == false) {
            return () -> {};
        }

        logger.debug("starting resource tracking for [task={}]", task.getId());

        // Add taskId to the threadContext, and give us a way to restore it later.
        ThreadContext threadContext = threadPool.getThreadContext();
        ThreadContext.StoredContext storedContext = threadContext.newStoredContext(true, List.of(Task.TASK_ID));
        threadContext.putTransient(Task.TASK_ID, task.getId());

        resourceAwareTasks.put(task.getId(), task);
        listeners.forEach(listener -> listener.onTaskResourceTrackingStarted(task));

        return storedContext;
    }

    /**
     * Stops resource tracking for the given task.
     */
    public void stopResourceTracking(Task task, ThreadContext.StoredContext storedContext) {
        logger.debug("stopping resource tracking for [task={}]", task.getId());
        // TODO: why do we need to stop resource tracking for current thread?

        resourceAwareTasks.remove(task.getId());
        listeners.forEach(listener -> listener.onTaskResourceTrackingStopped(task));

        // Must be restored at the end so that the taskId is not removed pre-maturely, which may lead to
        // accounting errors or race-conditions.
        storedContext.restore();
    }

    @Override
    public void onThreadExecutionStarted(long taskId, long threadId) {
        Task task = resourceAwareTasks.get(taskId);
        if (task == null) {
            logger.debug("thread execution started on task that no longer exists [taskId=" + taskId + " threadId=" + threadId + "]");
            return;
        }

        listeners.forEach(listener -> listener.onThreadExecutionStarted(task, threadId));
    }

    @Override
    public void onThreadExecutionStopped(long taskId, long threadId) {
        Task task = resourceAwareTasks.get(taskId);
        if (task == null) {
            logger.debug("thread execution stopped on task that no longer exists [taskId=" + taskId + " threadId=" + threadId + "]");
            return;
        }

        listeners.forEach(listener -> listener.onThreadExecutionStopped(task, threadId));
    }

    @Override
    public void onThreadExecutionStarted(Task task, long threadId) {
        assert threadId == Thread.currentThread().getId() : "threadId mentioned is not the same as the one working on it";
        task.startThreadResourceTracking(threadId, ResourceStatsType.WORKER_STATS, getThreadResourceUsageMetrics(threadId));
    }

    @Override
    public void onThreadExecutionStopped(Task task, long threadId) {
        assert threadId == Thread.currentThread().getId() : "threadId mentioned is not the same as the one working on it";
        task.stopThreadResourceTracking(threadId, ResourceStatsType.WORKER_STATS, getThreadResourceUsageMetrics(threadId));
    }

    @Override
    public void onTaskResourceTrackingStarted(Task task) {

    }

    @Override
    public void onTaskResourceUsageUpdated(Task task) {

    }

    @Override
    public void onTaskResourceTrackingStopped(Task task) {

    }
}
