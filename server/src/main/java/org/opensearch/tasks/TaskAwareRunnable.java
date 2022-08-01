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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps another runnable to provide updates on thread start/stop when working on a task.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {
    private static final Logger logger = LogManager.getLogger(TaskAwareRunnable.class);

    private final ThreadContext threadContext;
    private final Runnable original;
    private final List<AtomicReference<Listener>> listeners;

    public TaskAwareRunnable(ThreadContext threadContext, Runnable original, List<AtomicReference<Listener>> listeners) {
        this.threadContext = threadContext;
        this.original = original;
        this.listeners = listeners;
    }

    public void addListener(AtomicReference<Listener> listener) {
        Objects.requireNonNull(listener, "listener reference cannot be null");
        listeners.add(listener);
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

        if (task != null) {
            listeners.forEach(ref -> {
                try {
                    Listener listener = ref.get();
                    if (listener != null) {
                        listener.onThreadExecutionStarted(task, threadId);
                    }
                } catch (Exception ignored) {}
            });
        } else {
            logger.info("task missing in threadContext when thread execution started [threadId=" + threadId + "]");
        }

        original.run();
    }

    @Override
    public void onAfter() {
        long threadId = Thread.currentThread().getId();
        Task task = threadContext.getTransient(Task.TASK_REF);

        if (task != null) {
            listeners.forEach(ref -> {
                try {
                    Listener listener = ref.get();
                    if (listener != null) {
                        listener.onThreadExecutionStopped(task, threadId);
                    }
                } catch (Exception ignored) {}
            });
        } else {
            logger.info("task missing in threadContext when thread execution stopped [threadId=" + threadId + "]");
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
