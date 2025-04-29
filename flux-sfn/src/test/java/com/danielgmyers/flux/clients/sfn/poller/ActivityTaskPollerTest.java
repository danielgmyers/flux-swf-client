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

package com.danielgmyers.flux.clients.sfn.poller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.ActivityExecutionUtil;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskRequest;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskHeartbeatResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessResponse;
import software.amazon.awssdk.services.sfn.model.TaskDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException;

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

    private static final String ACTIVITY_ARN = SfnArnFormatter.activityArn("us-west-2", "123456789012",
                                                                           DummyWorkflow.class, DummyStep.class);
    private static final String IDENTITY = "unit";
    private static final String TASK_TOKEN = "task-token";

    private static final String METRIC_SUFFIX = DummyWorkflow.class.getSimpleName() + "." + DummyStep.class.getSimpleName();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private IMocksControl mockery;
    private SfnClient sfn;
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
        sfn = mockery.createMock(SfnClient.class);

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
        poller = new ActivityTaskPoller(metricsFactory, sfn, IDENTITY, ACTIVITY_ARN, workflow, step, executor);
    }

    @Test
    public void doesNothingIfNoWorkObjectReturned() throws InterruptedException {
        expectPoll(null);
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
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
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
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
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertEquals(1, pollThreadMetrics.getCounts().get(ActivityTaskPoller.NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void retriesIfRetryableClientException() throws InterruptedException {
        GetActivityTaskResponse task = GetActivityTaskResponse.builder().build();

        // throttle twice, then succeed
        GetActivityTaskRequest request = GetActivityTaskRequest.builder()
                .activityArn(ACTIVITY_ARN).workerName(IDENTITY).build();
        EasyMock.expect(sfn.getActivityTask(request))
            .andThrow(SdkClientException.builder().cause(new SSLException(new SocketException("Connection Closed")))
                .build())
            .times(2);

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertFalse(workerMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void submitRetryWhenStepReturnsRetry() throws Exception {
        Map<String, String> input = new HashMap<>();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.RETRY, null, "message", Collections.emptyMap());
        step.setStepResult(result);

        expectSubmitRetry(result);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(METRIC_SUFFIX, null)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void submitRetryWhenStepThrowsException() throws Exception {
        Map<String, String> input = new HashMap<>();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        RuntimeException e = new RuntimeException("Something went wrong!");
        step.setExceptionToThrow(e);

        expectSubmitRetry(StepResult.retry(e));

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing()); // false because the step threw an exception
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(METRIC_SUFFIX, e.getClass().getSimpleName())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsBeforeHeartbeatInterval() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        expectSubmitActivityCompleted(result);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(METRIC_SUFFIX, StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_ActivityTimedOutBeforeFirstHeartbeat() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());
        expectActivityTimedOutOnHeartbeat(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(METRIC_SUFFIX, InterruptedException.class.getSimpleName())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_ActivityDoesNotExistOnFirstHeartbeat() throws InterruptedException {
        Map<String, String> input = new HashMap<>();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());
        expectTaskDoesNotExistOnHeartbeat(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertFalse(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(METRIC_SUFFIX, InterruptedException.class.getSimpleName())).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsAfterOneHeartbeatInterval() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis());

        expectSubmitActivityCompleted(result);
        expectHeartbeat(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(METRIC_SUFFIX, StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void runsActivityExecutor_SucceedsAfterTwoHeartbeatIntervals() throws InterruptedException {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        GetActivityTaskResponse task = makeTask(input);
        expectPoll(task);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);
        step.setSleepDurationMillis(ActivityTaskPoller.HEARTBEAT_INTERVAL.plus(Duration.ofSeconds(1)).toMillis() * 2);

        expectSubmitActivityCompleted(result);
        expectHeartbeat(task);
        expectHeartbeat(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertTrue(step.didThing());
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityTaskPollTimeMetricName(METRIC_SUFFIX) + "Time"));
        Assertions.assertNotNull(pollThreadMetrics.getDurations().get(ActivityTaskPoller.WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertTrue(pollThreadMetrics.isClosed());
        Assertions.assertTrue(workerMetricsRequested);
        Assertions.assertEquals(1, workerThreadMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(METRIC_SUFFIX, StepResult.SUCCEED_RESULT_CODE)).longValue());
        Assertions.assertNotNull(workerThreadMetrics.getDurations().get(ActivityTaskPoller.formatActivityExecutionTimeMetricName(METRIC_SUFFIX)));
        Assertions.assertTrue(workerThreadMetrics.isClosed());
        Assertions.assertTrue(stepMetricsRequested);
        Assertions.assertTrue(stepMetrics.isClosed());
        mockery.verify();
    }

    @Test
    public void testPrepareRetryCause() {
        Assertions.assertNull(poller.prepareRetryCause(null));

        String shortString = "short string";
        Assertions.assertEquals(shortString, poller.prepareRetryCause(shortString));

        String maxLengthString = String.join("", Collections.nCopies(32768, "a"));
        Assertions.assertEquals(maxLengthString, poller.prepareRetryCause(maxLengthString));

        String shouldBeTruncated = String.join("", Collections.nCopies(32769, "t"));
        String truncatedString = shouldBeTruncated.substring(0, 32768 - ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION.length());
        truncatedString += ActivityTaskPoller.SUFFIX_INDICATING_TRUNCATION;
        Assertions.assertEquals(32768, truncatedString.length());
        Assertions.assertEquals(truncatedString, poller.prepareRetryCause(shouldBeTruncated));
    }

    private GetActivityTaskResponse makeTask(Map<String, String> input) {
        return GetActivityTaskResponse.builder()
                .taskToken(TASK_TOKEN)
                .input(StepAttributes.encode(input))
                .build();
    }

    private StepResult makeStepResult(StepResult.ResultAction resultAction, String stepResult, String message, Map<String, String> output) {
        return new StepResult(resultAction, stepResult, message).withAttributes(output);
    }

    private void expectPoll(GetActivityTaskResponse taskToReturn) {
        GetActivityTaskRequest request = GetActivityTaskRequest.builder()
                .activityArn(ACTIVITY_ARN).workerName(IDENTITY).build();
        EasyMock.expect(sfn.getActivityTask(request)).andReturn(taskToReturn);
    }

    private void expectSubmitActivityCompleted(StepResult result) {
        Map<String, String> aggregateOutput = new HashMap<>();

        for(Entry<String, Object> attr : result.getAttributes().entrySet()) {
            aggregateOutput.put(attr.getKey(), StepAttributes.encode(attr.getValue()));
        }

        aggregateOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());
        if(result.getMessage() != null) {
            aggregateOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        }

        SendTaskSuccessRequest successRequest = SendTaskSuccessRequest.builder().taskToken(TASK_TOKEN)
                .output(StepAttributes.encode(aggregateOutput)).build();
        EasyMock.expect(sfn.sendTaskSuccess(successRequest))
                .andReturn(SendTaskSuccessResponse.builder().build());
    }

    private void expectSubmitRetry(StepResult result) throws JsonProcessingException {
        String cause = null;
        if (result.getCause() != null) {
            StringWriter sw = new StringWriter();
            result.getCause().printStackTrace(new PrintWriter(sw));
            cause = sw.toString();
        } else if (result.getMessage() != null && !result.getMessage().isEmpty()) {
            cause = result.getMessage();
        }

        SendTaskFailureRequest failureRequest = SendTaskFailureRequest.builder()
                .taskToken(TASK_TOKEN)
                .error(ActivityTaskPoller.RETRY_ERROR_CODE)
                .cause(cause)
                .build();
        EasyMock.expect(sfn.sendTaskFailure(failureRequest)).andReturn(SendTaskFailureResponse.builder().build());
    }

    private void expectHeartbeat(GetActivityTaskResponse task) {
        SendTaskHeartbeatRequest request = SendTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
        SendTaskHeartbeatResponse response = SendTaskHeartbeatResponse.builder().build();
        EasyMock.expect(sfn.sendTaskHeartbeat(request)).andReturn(response);
    }

    private void expectTaskDoesNotExistOnHeartbeat(GetActivityTaskResponse task) {
        SendTaskHeartbeatRequest request = SendTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
        EasyMock.expect(sfn.sendTaskHeartbeat(request))
                .andThrow(TaskDoesNotExistException.builder().build());
    }

    private void expectActivityTimedOutOnHeartbeat(GetActivityTaskResponse task) {
        SendTaskHeartbeatRequest request = SendTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
        EasyMock.expect(sfn.sendTaskHeartbeat(request))
                .andThrow(TaskTimedOutException.builder().build());
    }

}
