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

    public TaskAwareRunnable(ThreadContext threadContext, Runnable original) {
        this.task = threadContext.getTransient(Task.THREAD_CONTEXT_TASK);
        this.original = original;
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
    protected void doRun() throws Exception {
        original.run();
    }

    @Override
    public Runnable unwrap() {
        return original;
    }

    public Task getTask() {
        return task;
    }
}
