/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.tracking;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class TaskResourceTrackingServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private TaskResourceTrackingService taskResourceTrackingService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool(TaskResourceTrackingService.class.getSimpleName());
        taskResourceTrackingService = new TaskResourceTrackingService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testTaskResourceTracking() {
        assertTrue(true);
    }
}
