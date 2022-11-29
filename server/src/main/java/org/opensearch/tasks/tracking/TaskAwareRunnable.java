/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.tracking;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.WrappedRunnable;
import org.opensearch.tasks.Task;

public class TaskAwareRunnable extends AbstractRunnable implements WrappedRunnable {

    @Nullable
    private final Task task; // May be null when task resource tracking is disabled.

    private final Runnable original;
    private final Listener listener;

    public TaskAwareRunnable(ThreadContext threadContext, Runnable original, Listener listener) {
        this.task = threadContext.getTransient(Task.THREAD_CONTEXT_TASK);
        this.original = original;
        this.listener = listener;
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
            super.onRejection(e);
        }
    }

    @Override
    public boolean isForceExecution() {
        return (original instanceof AbstractRunnable)
            ? ((AbstractRunnable) original).isForceExecution()
            : super.isForceExecution();
    }

    @Override
    public void onSubmit() {
        if (task != null) {
            listener.onRunnableSubmitted(task, Thread.currentThread().getId());
        }
    }

    @Override
    protected void doRun() throws Exception {
        if (task != null) {
            listener.onRunnableStarted(task, Thread.currentThread().getId());
        }

        original.run();
    }

    @Override
    public void onAfter() {
        if (task != null) {
            listener.onRunnableCompleted(task, Thread.currentThread().getId());
        }
    }

    @Override
    public Runnable unwrap() {
        return original;
    }

    public interface Listener {
        void onRunnableSubmitted(Task task, long threadId);

        void onRunnableStarted(Task task, long threadId);

        void onRunnableCompleted(Task task, long threadId);
    }
}
