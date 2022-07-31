/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

/**
 * Listener for events related to resource tracking of a task.
 */
public interface TaskResourceTrackingListener {
    /**
     * Invoked when thread execution starts for a task.
     */
    void onThreadExecutionStarted(Task task, long threadId);

    /**
     * Invoked when thread execution stops for a task.
     */
    void onThreadExecutionStopped(Task task, long threadId);

    /**
     * Invoked when task resource tracking is started.
     */
    void onTaskResourceTrackingStarted(Task task);

    /**
     * Invoked when task resource usage metrics are updated.
     */
    void onTaskResourceUsageUpdated(Task task);

    /**
     * Invoked when task resource tracking is stopped.
     */
    void onTaskResourceTrackingStopped(Task task);
}
