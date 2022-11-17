/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.backpressure.settings.NodeDuressSettings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SearchBackpressureIT extends OpenSearchIntegTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    public void testSearchShardTaskCancellation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        client().execute(TestTransportAction.ACTION, new TestRequest(), new ActionListener<>() {
            @Override
            public void onResponse(TestResponse testResponse) {
                fail("SearchShardTask should have been cancelled");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(TaskCancelledException.class, e.getClass());
                assertTrue(e.getMessage().contains("elapsed time exceeded"));
                latch.countDown();
            }
        });

        latch.await();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced")
            .put(NodeDuressSettings.SETTING_CPU_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_HEAP_THRESHOLD.getKey(), 0.0)
            .put(NodeDuressSettings.SETTING_NUM_SUCCESSIVE_BREACHES.getKey(), 3)
            .put(SearchShardTaskSettings.SETTING_TOTAL_HEAP_PERCENT_THRESHOLD.getKey(), 0)
            .put(ElapsedTimeTracker.SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.getKey(), 5000)
            .build();
    }

    public static class TestRequest extends ActionRequest {
        public TestRequest() {
        }

        public TestRequest(StreamInput in) {
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new SearchShardTask(id, type, action, "", parentTaskId, headers);
        }
    }

    public static class TestResponse extends ActionResponse {
        public TestResponse() {
        }

        public TestResponse(StreamInput in) {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    public static class TestTransportAction extends HandledTransportAction<TestRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_action", TestResponse::new);

        @Inject
        public TestTransportAction(TransportService transportService, NodeClient client, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestRequest::new, ThreadPool.Names.GENERIC);
        }

        @Override
        protected void doExecute(Task task, TestRequest request, ActionListener<TestResponse> listener) {
            try {
                SearchShardTask searchShardTask = (SearchShardTask) task;
                long startTime = System.nanoTime();

                // Doing a busy-wait until task cancellation or timeout.
                do {
                    doWork(request);
                    Thread.sleep(500);
                } while (searchShardTask.isCancelled() == false && (System.nanoTime() - startTime) < TIMEOUT.getNanos());

                if (searchShardTask.isCancelled()) {
                    throw new TaskCancelledException(searchShardTask.getReasonCancelled());
                } else {
                    listener.onResponse(new TestResponse());
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void doWork(TestRequest request) throws InterruptedException {
            // Do some expensive work here (run a tight loop to exhaust CPU, or allocate bytes to exhaust heap).
            // You can add some parameters to TestRequest to identify what kind of work you want to simulate here.
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Collections.singletonList(new ActionHandler<>(TestTransportAction.ACTION, TestTransportAction.class));
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Collections.singletonList(TestTransportAction.ACTION);
        }
    }
}
