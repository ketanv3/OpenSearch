/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.WrappedRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Wraps another runnable to provide updates on thread start/stop when working on a task.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {
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
        Objects.requireNonNull(task, "task not found in threadContext [threadId=" + threadId + "]");

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
