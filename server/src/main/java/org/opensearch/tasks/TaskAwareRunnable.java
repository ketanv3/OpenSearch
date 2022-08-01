/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.WrappedRunnable;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps another runnable to provide updates on thread start/stop when working on a task.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {
    private static final Logger logger = LogManager.getLogger(TaskAwareRunnable.class);

    private final ThreadContext threadContext;
    private final Runnable original;
    private final List<Listener> listeners;

    public TaskAwareRunnable(ThreadContext threadContext, Runnable original, List<Listener> listeners) {
        this.threadContext = threadContext;
        this.original = original;
        this.listeners = listeners;
    }

    @Override
    public void onFailure(Exception e) {
        ExceptionsHelper.reThrowIfNotNull(e);
    }

    @Override
    public void onRejection(Exception e) {
        if (original instanceof AbstractRunnable) {
            ((AbstractRunnable) original).onRejection(e);
        } else {
            ExceptionsHelper.reThrowIfNotNull(e);
        }
    }

    @Override
    public boolean isForceExecution() {
        return original instanceof AbstractRunnable && ((AbstractRunnable) original).isForceExecution();
    }

    @Override
    protected void doRun() throws Exception {
        long threadId = Thread.currentThread().getId();
        Task task = threadContext.getTransient(Task.TASK_REF);

        // If the task doesn't exist in the threadContext, execute the runnable as a regular AbstractRunnable
        // without sending updates to the listeners.
        if (task == null) {
            logger.debug("task not found in threadContext [threadId={}], skipping updates", threadId);
            original.run();
            return;
        }

        List<Exception> listenerExceptions = new ArrayList<>();
        listeners.forEach(listener -> {
            try {
                listener.onThreadExecutionStarted(task, threadId);
            } catch (Exception e) {
                listenerExceptions.add(e);
            }
        });
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(listenerExceptions);

        try {
            original.run();
        } finally {
            listeners.forEach(listener -> {
                try {
                    listener.onThreadExecutionStopped(task, threadId);
                } catch (Exception e) {
                    listenerExceptions.add(e);
                }
            });
            ExceptionsHelper.maybeThrowRuntimeAndSuppress(listenerExceptions);
        }
    }

    @Override
    public Runnable unwrap() {
        return original;
    }

    /**
     * Listener for events related to thread execution for a task.
     */
    public interface Listener {
        /**
         * Invoked when thread execution starts for a task.
         */
        void onThreadExecutionStarted(Task task, long threadId);

        /**
         * Invoked when thread execution stops for a task.
         */
        void onThreadExecutionStopped(Task task, long threadId);
    }
}
