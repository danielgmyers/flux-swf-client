/*
 *   Copyright Flux Contributors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.danielgmyers.flux.clients.swf.poller;

import java.net.SocketException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.ex.UnrecognizedTaskException;
import com.danielgmyers.flux.poller.ActivityExecutionUtil;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskRequest;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatRequest;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatResponse;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCanceledRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCanceledResponse;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCompletedResponse;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskFailedRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskFailedResponse;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;

@Execution(ExecutionMode.CONCURRENT)
public class ActivityTaskPollerTest {

    public static class DummyStep implements WorkflowStep {

        private boolean didThing = false;
        private RuntimeException exceptionToThrow = null;
        private StepResult result = null;
        private long sleepDurationMillis = 0;

        public void setExceptionToThrow(RuntimeException e) {
            this.exceptionToThrow = e;
        }

        public void setStepResult(StepResult result) {
            this.result = result;
        }

        public void setSleepDurationMillis(long sleepDurationMillis) {
            this.sleepDurationMillis = sleepDurationMillis;
        }

        @StepApply
        public StepResult doThing() throws InterruptedException {
            if(exceptionToThrow != null) {
                throw exceptionToThrow;
            }

            if(sleepDurationMillis > 0) {
                Thread.sleep(sleepDurationMillis);
            }

            didThing = true;
            return result;
        }

        public boolean didThing() {
            return didThing;
        }
    }

    public static class DummyWorkflow implements Workflow {

        private final WorkflowGraph graph;

        public DummyWorkflow(DummyStep dummyStep) {
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(dummyStep);
            builder.alwaysClose(dummyStep);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    private static final String DOMAIN = "test";
    private static final String IDENTITY = "unit";
    private static final String TASK_TOKEN = "task-token";

    private IMocksControl mockery;
    private SwfClient swf;
    private DummyStep step;
    private String stepName;

    private MetricRecorderFactory metricsFactory;
    private InMemoryMetricRecorder pollThreadMetrics;
    private InMemoryMetricRecorder workerThreadMetrics;
    private InMemoryMetricRecorder stepMetrics;

    private boolean workerMetricsRequested;
    private boolean stepMetricsRequested;

    private Map<String, WorkflowStep> steps;
    private ActivityTaskPoller poller;

    private Map<String, Workflow> workflows;
    private Workflow workflow;
    private String workflowName;

    private BlockOnSubmissionThreadPoolExecutor executor;

    @BeforeEach
    public void setup() {
        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        step = new DummyStep();

        workflow = new DummyWorkflow(step);
        workflowName = TaskNaming.workflowName(workflow);

        stepName = TaskNaming.activityName(workflowName, step);

        workflows = new HashMap<>();
        workflows.put(workflowName, workflow);

        steps = new HashMap<>();
        steps.put(stepName, step);

        pollThreadMetrics = new InMemoryMetricRecorder("ActivityTaskPoller");
        workerThreadMetrics = new InMemoryMetricRecorder("ActivityTaskPoller.executeWithHeartbeat");
        workerMetricsRequested = false;
        stepMetrics = new InMemoryMetricRecorder(stepName);
        stepMetricsRequested = false;

        metricsFactory = new MetricRecorderFactory() {
            private int requestNo = 0;
            @Override
            public MetricRecorder newMetricRecorder(String operation, Clock clock) {
                requestNo++;
                if (requestNo == 1) {
                    return pollThreadMetrics;
                } else if (requestNo == 2) {
                    workerMetricsRequested = true;
                    return workerThreadMetrics;
                } else if (requestNo == 3) {
                    stepMetricsRequested = true;
                    return stepMetrics;
                } else {
                    throw new RuntimeException("Only expected three calls to newMetrics()");
                }
            }
        };

        executor = new BlockOnSubmissionThreadPoolExecutor(1, "executor");
        poller = new ActivityTaskPoller(metricsFactory, swf, DOMAIN, Workflow.DEFAULT_TASK_LIST_NAME,
                                        IDENTITY, workflows, steps, executor);
    }

    @Test
    public void doesNothingIfNoWorkObjectReturned() throws InterruptedException {
        expectPoll(null);
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollThreadMetrics.getCounts().get(ActivityTaskPoller.NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void doesNothingIfNoTaskTokenReturned() throws InterruptedException {
        expectPoll(makeTask(null).toBuilder().taskToken(null).build());
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollThreadMetrics.getCounts().get(ActivityTaskPoller.NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void doesNothingIfBlankTaskTokenReturned() throws InterruptedException {
        expectPoll(makeTask(null).toBuilder().taskToken("").build());
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollThreadMetrics.getCounts().get(ActivityTaskPoller.NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void throwsIfUnrecognizedActivitySpecifiedInTask() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        String activityName = "fakeStep";
        ActivityType fakeType = ActivityType.builder().name(activityName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();
        expectPoll(makeTask(input).toBuilder().activityType(fakeType).build());
        mockery.replay();
        try {
            poller.run();
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
            Assertions.fail();
        } catch(UnrecognizedTaskException e) {
            // expected
        }
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollThreadMetrics.getCounts().get(ActivityTaskPoller.formatUnrecognizedActivityTaskMetricName(activityName)).longValue());
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void retriesIfRetryableClientException() throws InterruptedException {
        PollForActivityTaskResponse task = PollForActivityTaskResponse.builder().build();

        // throttle twice, then succeed
        PollForActivityTaskRequest request = PollForActivityTaskRequest.builder().domain(DOMAIN)
            .taskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
            .identity(IDENTITY).build();
        EasyMock.expect(swf.pollForActivityTask(request))
            .andThrow(SdkClientException.builder().cause(new SSLException(new SocketException("Connection Closed")))
                .build())
            .times(2);

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void submitRetryWhenStepReturnsRetry() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        String message = "hmm";
        StepResult result = makeStepResult(StepResult.ResultAction.RETRY, null, message, Collections.emptyMap());
        step.setStepResult(result);

        expectSubmitRetry(message, null);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(), null)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void submitRetryWhenStepReturnsRetry_TruncateMessageIfTooLong() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        String message = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        message = message + message + message + message + message + message;
        Assertions.assertTrue(message.length() > 256);

        StepResult result = makeStepResult(StepResult.ResultAction.RETRY, null, message, Collections.emptyMap());
        step.setStepResult(result);

        expectSubmitRetry(message, null);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(), null)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsBeforeHeartbeatInterval() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        expectSubmitActivityCompleted(input, result);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(), StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsBeforeHeartbeatInterval_CustomTaskList() throws InterruptedException {
        String customTaskListName = "foobar";

        ActivityTaskPoller customTaskList = new ActivityTaskPoller(metricsFactory, swf, DOMAIN, customTaskListName, IDENTITY, workflows, steps, executor);

        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task, customTaskListName);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        expectSubmitActivityCompleted(input, result);

        mockery.replay();
        customTaskList.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + customTaskListName));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(), StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsAfterOneHeartbeatInterval() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());

        expectSubmitActivityCompleted(input, result);
        expectHeartbeat(task, false);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(), StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsAfterTwoHeartbeatIntervals() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis() * 2);

        expectSubmitActivityCompleted(input, result);
        expectHeartbeat(task, false);
        expectHeartbeat(task, false);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(), StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_ActivityCancelledOnFirstHeartbeat() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());

        expectSubmitActivityCancelled();
        expectHeartbeat(task, true);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(), InterruptedException.class.getSimpleName())).longValue());
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityTaskPoller.formatActivityTaskCancelledMetricName(task.activityType().name())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_ActivityTimedOutBeforeFirstHeartbeat() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());
        expectSubmitActivityCancelled();
        expectActivityTimedOut(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(), InterruptedException.class.getSimpleName())).longValue());
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityTaskPoller.formatActivityTaskCancelledMetricName(task.activityType().name())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_ActivityCancelledOnSecondHeartbeat() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis() * 2);

        expectSubmitActivityCancelled();

        expectHeartbeat(task, false);
        expectHeartbeat(task, true);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(), InterruptedException.class.getSimpleName())).longValue());
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityTaskPoller.formatActivityTaskCancelledMetricName(task.activityType().name())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(task.activityType().name())));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void testPrepareRetryReason() {
        Assertions.assertNull(poller.prepareRetryReason(null));

        String shortString = "short string";
        Assertions.assertEquals(shortString, poller.prepareRetryReason(shortString));

        String maxLengthString = String.join("", Collections.nCopies(256, "a"));
        Assertions.assertEquals(maxLengthString, poller.prepareRetryReason(maxLengthString));

        String shouldBeTruncated = String.join("", Collections.nCopies(257, "t"));
        String truncatedString = shouldBeTruncated.substring(0, 256 - ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION.length());
        truncatedString += ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION;
        Assertions.assertEquals(256, truncatedString.length());
        Assertions.assertEquals(truncatedString, poller.prepareRetryReason(shouldBeTruncated));
    }

    @Test
    public void testPrepareRetryDetails() {
        Assertions.assertNull(poller.prepareRetryDetails(null));

        String shortString = "short string";
        Assertions.assertEquals(shortString, poller.prepareRetryDetails(shortString));

        String maxLengthString = String.join("", Collections.nCopies(32768, "a"));
        Assertions.assertEquals(maxLengthString, poller.prepareRetryDetails(maxLengthString));

        String shouldBeTruncated = String.join("", Collections.nCopies(32769, "t"));
        String truncatedString = shouldBeTruncated.substring(0, 32768 - ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION.length());
        truncatedString += ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION;
        Assertions.assertEquals(32768, truncatedString.length());
        Assertions.assertEquals(truncatedString, poller.prepareRetryDetails(shouldBeTruncated));
    }

    private PollForActivityTaskResponse makeTask(Map<String, String> input) {
        return PollForActivityTaskResponse.builder()
                .activityId("some-activity-id")
                .activityType(ActivityType.builder().name(stepName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build())
                .taskToken(TASK_TOKEN)
                .input(StepAttributes.encode(input))
                .workflowExecution(WorkflowExecution.builder().workflowId("some-workflow-id").runId("run-id").build())
                .build();
    }

    private StepResult makeStepResult(StepResult.ResultAction resultAction, String stepResult, String message, Map<String, String> output) {
        return new StepResult(resultAction, stepResult, message).withAttributes(output);
    }

    private void expectPoll(PollForActivityTaskResponse taskToReturn) {
        expectPoll(taskToReturn, Workflow.DEFAULT_TASK_LIST_NAME);
    }

    private void expectPoll(PollForActivityTaskResponse taskToReturn, String taskList) {
        PollForActivityTaskRequest request = PollForActivityTaskRequest.builder().domain(DOMAIN)
                .taskList(TaskList.builder().name(taskList).build()).identity(IDENTITY).build();
        EasyMock.expect(swf.pollForActivityTask(request)).andReturn(taskToReturn);
    }

    private void expectSubmitActivityCompleted(Map<String, String> input, StepResult result) {
        Map<String, String> aggregateOutput = new HashMap<>(input);

        for(Entry<String, Object> attr : result.getAttributes().entrySet()) {
            aggregateOutput.put(attr.getKey(), StepAttributes.encode(attr.getValue()));
        }

        aggregateOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());
        if(result.getMessage() != null) {
            aggregateOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        }

        RespondActivityTaskCompletedRequest rac = RespondActivityTaskCompletedRequest.builder().taskToken(TASK_TOKEN)
                .result(StepAttributes.encode(aggregateOutput)).build();
        EasyMock.expect(swf.respondActivityTaskCompleted(rac))
                .andReturn(RespondActivityTaskCompletedResponse.builder().build());
    }

    private void expectSubmitActivityCancelled() {
        EasyMock.expect(swf.respondActivityTaskCanceled(RespondActivityTaskCanceledRequest.builder().taskToken(TASK_TOKEN).build()))
                .andReturn(RespondActivityTaskCanceledResponse.builder().build());
    }

    private void expectSubmitRetry(String message, String details) {
        RespondActivityTaskFailedRequest raf = RespondActivityTaskFailedRequest.builder()
                .taskToken(TASK_TOKEN)
                .reason(poller.prepareRetryReason(message)) // prepareRetryReason is tested independently
                .details(poller.prepareRetryDetails(details)) // prepareRetryDetails is tested independently
                .build();
        Assertions.assertTrue(raf.reason() == null || raf.reason().length() <= 256);
        Assertions.assertTrue(raf.details() == null || raf.details().length() <= 32768);
        EasyMock.expect(swf.respondActivityTaskFailed(raf)).andReturn(RespondActivityTaskFailedResponse.builder().build());
    }

    private void expectHeartbeat(PollForActivityTaskResponse task, boolean cancelRequested) {
        RecordActivityTaskHeartbeatRequest request = RecordActivityTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
        RecordActivityTaskHeartbeatResponse response = RecordActivityTaskHeartbeatResponse.builder().cancelRequested(cancelRequested).build();
        EasyMock.expect(swf.recordActivityTaskHeartbeat(request)).andReturn(response);
    }

    private void expectActivityTimedOut(PollForActivityTaskResponse task) {
        RecordActivityTaskHeartbeatRequest request = RecordActivityTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
        // SWF throws UnknownResourceException when recordActivityTaskHeartbeat is called for a task that has timed out.
        EasyMock.expect(swf.recordActivityTaskHeartbeat(request))
                .andThrow(UnknownResourceException.builder().build());
    }

}
