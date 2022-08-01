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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps another runnable to provide updates on thread start/stop when working on a task.
 */
public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {
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
        Long taskId = threadContext.getTransient(Task.TASK_ID);
        long threadId = Thread.currentThread().getId();

        if (taskId != null) {
            listeners.forEach(ref -> {
                try {
                    Listener listener = ref.get();
                    if (listener != null) {
                        listener.onThreadExecutionStarted(taskId, threadId);
                    }
                } catch (Exception ignored) {}
            });
        }

        original.run();
    }

    @Override
    public void onAfter() {
        Long taskId = threadContext.getTransient(Task.TASK_ID);
        long threadId = Thread.currentThread().getId();

        if (taskId != null) {
            listeners.forEach(ref -> {
                try {
                    Listener listener = ref.get();
                    if (listener != null) {
                        listener.onThreadExecutionStopped(taskId, threadId);
                    }
                } catch (Exception ignored) {}
            });
        }
    }

    @Override
    public Runnable unwrap() {
        return original;
    }

    public interface Listener {
        void onThreadExecutionStarted(long taskId, long threadId);

        void onThreadExecutionStopped(long taskId, long threadId);
    }
}
