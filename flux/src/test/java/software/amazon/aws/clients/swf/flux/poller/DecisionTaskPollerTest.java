/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.aws.clients.swf.flux.poller;

import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.FluxCapacitorImpl;
import software.amazon.aws.clients.swf.flux.metrics.InMemoryMetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.poller.signals.DelayRetrySignalData;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestBranchStep;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestBranchingWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPartitionedStep;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPartitionedStepUsesPartitionIdGeneratorResult;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPeriodicWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPeriodicWorkflowCustomTaskList;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepHasOptionalInputAttribute;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepOne;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepReturnsCustomResultCode;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepTwo;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowCustomTaskList;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowDoesntHandleCustomResultCode;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithAlwaysTransition;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithFailureTransition;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithPartitionedStep;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithStepCustomHeartbeatTimeout;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.util.RetryUtils;
import software.amazon.aws.clients.swf.flux.wf.Periodic;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphNode;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.CancelTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.CancelWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.CompleteWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ContinueAsNewWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.DecisionType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskRequest;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RequestCancelActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedResponse;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;

public class DecisionTaskPollerTest {

    private static final String DOMAIN = "test";
    private static final String IDENTITY = "unit";

    private Workflow workflow;
    private String workflowName;

    private Workflow periodicWorkflow;
    private String periodicWorkflowName;

    private Workflow workflowWithPartitionedStep;
    private String workflowWithPartitionedStepName;

    private Workflow workflowWithCustomHeartbeatTimeout;
    private String workflowWithCustomHeartbeatTimeoutName;

    private MetricRecorderFactory metricsFactory;
    private InMemoryMetricRecorder pollMetrics;
    private InMemoryMetricRecorder deciderMetrics;
    private InMemoryMetricRecorder stepMetrics;

    private boolean deciderMetricsRequested;
    private boolean stepMetricsRequested;

    private IMocksControl mockery;
    private SwfClient swf;

    private DecisionTaskPoller poller;

    private BlockOnSubmissionThreadPoolExecutor executor;

    @Before
    public void setup() {
        workflow = new TestWorkflow();
        workflowName = TaskNaming.workflowName(workflow);

        periodicWorkflow = new TestPeriodicWorkflow();
        periodicWorkflowName = TaskNaming.workflowName(periodicWorkflow);

        workflowWithPartitionedStep = new TestWorkflowWithPartitionedStep();
        workflowWithPartitionedStepName = TaskNaming.workflowName(workflowWithPartitionedStep);

        workflowWithCustomHeartbeatTimeout = new TestWorkflowWithStepCustomHeartbeatTimeout();
        workflowWithCustomHeartbeatTimeoutName = TaskNaming.workflowName(workflowWithCustomHeartbeatTimeout);

        pollMetrics = new InMemoryMetricRecorder("ActivityTaskPoller");

        deciderMetrics = new InMemoryMetricRecorder("ActivityTaskPoller.executeDecisionTask");
        deciderMetricsRequested = false;

        stepMetrics = new InMemoryMetricRecorder("stepName");
        stepMetricsRequested = false;

        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        Map<String, Workflow> workflows = new HashMap<>();
        workflows.put(workflowName, workflow);

        Map<String, WorkflowStep> activities = new HashMap<>();
        for(Class<? extends WorkflowStep> step : workflow.getGraph().getNodes().keySet()) {
            WorkflowStep impl = workflow.getGraph().getNodes().get(step).getStep();
            activities.put(TaskNaming.activityName(workflowName, impl), impl);
        }
        for(Class<? extends WorkflowStep> step : periodicWorkflow.getGraph().getNodes().keySet()) {
            WorkflowStep impl = workflow.getGraph().getNodes().get(step).getStep();
            activities.put(TaskNaming.activityName(periodicWorkflowName, impl), impl);
        }

        metricsFactory = new MetricRecorderFactory() {
            private int requestNo = 0;
            @Override
            public MetricRecorder newMetricRecorder(String operation) {
                requestNo++;
                if (requestNo == 1) {
                    return pollMetrics;
                } else if (requestNo == 2) {
                    deciderMetricsRequested = true;
                    return deciderMetrics;
                } else if (requestNo == 3) {
                    stepMetricsRequested = true;
                    return stepMetrics;
                } else {
                    throw new RuntimeException("Only expected three calls to newMetrics()");
                }
            }
        };

        executor = new BlockOnSubmissionThreadPoolExecutor(1);
        poller = new DecisionTaskPoller(metricsFactory, swf, DOMAIN, Workflow.DEFAULT_TASK_LIST_NAME, IDENTITY,
                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE, workflows, activities, executor);
    }

    @Test
    public void doesNothingIfNoWorkObjectReturned() throws InterruptedException {
        expectPoll(null);
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void doesNothingIfNoTaskTokenReturned() throws InterruptedException {
        PollForDecisionTaskResponse task = PollForDecisionTaskResponse.builder().build();

        expectPoll(task);
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(deciderMetricsRequested);
        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void doesNothingIfBlankTaskTokenReturned() throws InterruptedException {
        PollForDecisionTaskResponse task = PollForDecisionTaskResponse.builder().taskToken("").build();

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(deciderMetricsRequested);
        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void retriesIfThrottled_ExceptionHasThrottlingStatusCode() throws InterruptedException {
        PollForDecisionTaskResponse task = PollForDecisionTaskResponse.builder().build();

        // throttle twice, then succeed
        PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder().domain(DOMAIN)
                .taskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
                .identity(IDENTITY).reverseOrder(true).build();
        EasyMock.expect(swf.pollForDecisionTask(request))
                .andThrow(SdkServiceException.builder().statusCode(HttpStatusCode.THROTTLING).message("throttled").build())
                .times(2);

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(deciderMetricsRequested);
        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void retriesIfThrottled_ExceptionHasThrottlingMessage() throws InterruptedException {
        PollForDecisionTaskResponse task = PollForDecisionTaskResponse.builder().build();

        // throttle twice, then succeed
        PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder().domain(DOMAIN)
                .taskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
                .identity(IDENTITY).reverseOrder(true).build();
        EasyMock.expect(swf.pollForDecisionTask(request))
                .andThrow(SdkServiceException.builder().statusCode(HttpStatusCode.BAD_REQUEST).message("Rate exceeded").build())
                .times(2);

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(deciderMetricsRequested);
        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void retriesIfRetryableClientException() throws InterruptedException {
        PollForDecisionTaskResponse task = PollForDecisionTaskResponse.builder().build();

        // socket exception twice, then succeed
        PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder().domain(DOMAIN)
            .taskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
            .identity(IDENTITY).reverseOrder(true).build();
        EasyMock.expect(swf.pollForDecisionTask(request))
            .andThrow(SdkClientException.builder().cause(new SSLException(new SocketException("Connection Closed")))
                .build())
            .times(2);

        expectPoll(task);

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assert.assertTrue(pollMetrics.isClosed());

        Assert.assertFalse(deciderMetricsRequested);
        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void testRun_SubmitsDecisionResult() throws InterruptedException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, Instant.now());
        WorkflowState state = history.buildCurrentState();
        expectPoll(history.buildDecisionTask());

        // for the purposes of this test we don't care what the actual request contains
        EasyMock.expect(swf.respondDecisionTaskCompleted(EasyMock.isA(RespondDecisionTaskCompletedRequest.class)))
                .andReturn(RespondDecisionTaskCompletedResponse.builder().build());

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assert.assertTrue(pollMetrics.isClosed());
        Assert.assertTrue(deciderMetricsRequested);
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatDecisionTaskEventHistoryPageCountMetricName(workflowName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assert.assertTrue(deciderMetrics.isClosed());

        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void testRun_SubmitsDecisionResult_CustomTaskList() throws InterruptedException {
        Workflow workflowWithCustomTaskList = new TestWorkflowCustomTaskList();
        String workflowWithCustomTaskListName = TaskNaming.workflowName(workflowWithCustomTaskList);

        String customTaskListName = workflowWithCustomTaskList.taskList();

        Map<String, Workflow> workflows = new HashMap<>();
        workflows.put(workflowWithCustomTaskListName, workflowWithCustomTaskList);

        Map<String, WorkflowStep> activities = new HashMap<>();
        for(Class<? extends WorkflowStep> step : workflowWithCustomTaskList.getGraph().getNodes().keySet()) {
            WorkflowStep impl = workflowWithCustomTaskList.getGraph().getNodes().get(step).getStep();
            activities.put(TaskNaming.activityName(workflowWithCustomTaskListName, impl), impl);
        }

        DecisionTaskPoller pollerCustomTaskList = new DecisionTaskPoller(metricsFactory, swf, DOMAIN,
                                                                         customTaskListName, IDENTITY,
                                                                         FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                         workflows, activities, executor);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithCustomTaskList, Instant.now());
        WorkflowState state = history.buildCurrentState();
        expectPoll(history.buildDecisionTask(), workflowWithCustomTaskList.taskList());

        // for the purposes of this test we don't care what the actual request contains
        EasyMock.expect(swf.respondDecisionTaskCompleted(EasyMock.isA(RespondDecisionTaskCompletedRequest.class)))
                .andReturn(RespondDecisionTaskCompletedResponse.builder().build());

        mockery.replay();
        pollerCustomTaskList.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assert.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + customTaskListName));
        Assert.assertTrue(pollMetrics.isClosed());
        Assert.assertTrue(deciderMetricsRequested);
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatDecisionTaskEventHistoryPageCountMetricName(workflowWithCustomTaskListName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assert.assertTrue(deciderMetrics.isClosed());

        Assert.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void decide_scheduleFirstStepWhenNoCurrentStep() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(20), input);
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, null, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, workflow.getGraph().getFirstStep());
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(15), input);

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.success().withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>();
        expectedInput.putAll(input);
        expectedInput.putAll(output);

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertFalse(closeEvent.eventTimestamp().isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assert.assertEquals(closeEvent.eventTimestamp().toEpochMilli() - startEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_recordMarkerAndRetryTimerIfUnknownResultCode() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(15), input);

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.complete("custom-result-code", "Graph doesn't handle this result code").withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });

        Assert.assertEquals(2, response.decisions().size());
        Decision markerDecision = response.decisions().get(0);
        Decision timerDecision = response.decisions().get(1);

        Assert.assertEquals(DecisionType.RECORD_MARKER, markerDecision.decisionType());
        Assert.assertEquals(DecisionType.START_TIMER, timerDecision.decisionType());

        StartTimerDecisionAttributes timerAttrs = timerDecision.startTimerDecisionAttributes();
        Assert.assertEquals(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_ID, timerAttrs.timerId());
        Assert.assertEquals(Long.toString(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_DELAY.getSeconds()),
                            timerAttrs.startToFireTimeout());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.UNKNOWN_RESULT_CODE_METRIC_BASE).longValue());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowMetricName(workflowName)).longValue());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowStepMetricName(activityName)).longValue());

        mockery.verify();
    }

    @Test
    public void decide_recordMarkerAndRetryTimerIfUnknownResultCode_DoNotCreateDuplicateTimer() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(15), input);

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.complete("custom-result-code", "Graph doesn't handle this result code").withAttributes(output));
        HistoryEvent timerEvent = history.startTimer(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_ID,
                                                     DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_DELAY);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });

        Assert.assertEquals(1, response.decisions().size());
        Decision markerDecision = response.decisions().get(0);

        Assert.assertEquals(DecisionType.RECORD_MARKER, markerDecision.decisionType());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.UNKNOWN_RESULT_CODE_METRIC_BASE).longValue());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowMetricName(workflowName)).longValue());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowStepMetricName(activityName)).longValue());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds_PreviousScheduleAttemptFailed() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(15), input);

        WorkflowStep firstStep = workflow.getGraph().getFirstStep();
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        history.recordScheduleAttemptFailed();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, firstStep, state.getWorkflowId(), state,
                FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });

        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);

        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>();
        expectedInput.putAll(input);
        expectedInput.putAll(output);
        expectedInput.remove(StepAttributes.RESULT_CODE);

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        Assert.assertEquals(closeEvent.eventTimestamp().toEpochMilli() - startEvent.eventTimestamp().toEpochMilli(),
                deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepResultCodeForcedBySignal() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent startEvent = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        HistoryEvent timerEvent = history.startRetryTimer(Duration.ofSeconds(10));

        history.recordForceResultSignal(StepResult.SUCCEED_RESULT_CODE);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();

        // we expect two decisions: one for scheduling the next step, and one for canceling the retry timer for the current step.
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);
        List<Decision> decisions = response.decisions();

        Assert.assertEquals(1, decisions.size());
        Decision decision = decisions.get(0);

        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        Assert.assertEquals(now.toEpochMilli() - startEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis(),
                            1000.0 /* account for timing variance in the test execution */);
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = workflowWithPartitionedStep.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, firstStep);
        Assert.assertEquals(closeEvent.eventTimestamp().toEpochMilli() - startEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assert.assertEquals(partitionIds.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assert.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        Assert.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            expectedInput.putAll(output);

            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

            // because this is the first attempt of the step and we don't control 'now' from the unit test,
            // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
            // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
            // and then save it to the input map for the actual comparison.
            Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
            Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
            Assert.assertNotNull(attemptTime);
            // the attempt time should be before or equal to the close event time.
            Assert.assertFalse(closeEvent.eventTimestamp().isAfter(attemptTime.toInstant()));
            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

            String activityId = TaskNaming.createActivityId(stepTwo, 0, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, stepTwo,
                    expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds_PartitionIdGeneratorAddsAttribute() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");

        Workflow wf = () -> {
            WorkflowStep stepOne = new TestStepOne();
            WorkflowStep partitionedStep = new TestPartitionedStepUsesPartitionIdGeneratorResult(partitionIds);
            WorkflowGraphBuilder b = new WorkflowGraphBuilder(stepOne);
            b.alwaysTransition(stepOne, partitionedStep);

            b.addStep(partitionedStep);
            b.alwaysClose(partitionedStep);

            return b.build();
        };

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(wf, now.minusSeconds(15), input);

        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent closeEvent = history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = wf.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(wf, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics, (o) -> stepMetrics);
        WorkflowStep stepTwo = wf.getGraph().getNodes().get(TestPartitionedStepUsesPartitionIdGeneratorResult.class).getStep();
        validateExecutionContext(response.executionContext(), wf, stepTwo);

        String activityName = TaskNaming.activityName(wf, firstStep);
        Assert.assertEquals(closeEvent.eventTimestamp().toEpochMilli() - startEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assert.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        Assert.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            expectedInput.putAll(output);

            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

            // the partition id generator should have added an extra attribute
            expectedInput.put(TestPartitionedStepUsesPartitionIdGeneratorResult.PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE,
                              StepAttributes.encode(TestPartitionedStepUsesPartitionIdGeneratorResult.PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE));

            // because this is the first attempt of the step and we don't control 'now' from the unit test,
            // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
            // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
            // and then save it to the input map for the actual comparison.
            Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
            Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
            Assert.assertNotNull(attemptTime);
            Assert.assertTrue(closeEvent.eventTimestamp().isBefore(attemptTime.toInstant()));
            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

            String activityId = TaskNaming.createActivityId(stepTwo, 0, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(wf, stepTwo,
                                                                                expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStepRetryTimers_AllPartitionsNeedRetry() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);
            history.recordActivityResult(partitionId, StepResult.retry());
        }

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assert.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assert.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assert.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStepRetryTimers_OnePartitionSucceeded() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String succeededPartition = "p1";
        partitionIds.add(succeededPartition);
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Set<String> retriedPartitions = new HashSet<>(partitionIds);
        retriedPartitions.remove(succeededPartition);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);

            StepResult result = (partitionId.equals(succeededPartition) ? StepResult.success() : StepResult.retry());
            history.recordActivityResult(partitionId, result);
        }

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assert.assertEquals(retriedPartitions.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(retriedPartitions);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assert.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assert.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStep_OnePartitionSucceeded_OnePartitionScheduleActivityFailed() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String succeededPartition = "p1";
        String failedPartition = "p2";
        partitionIds.add(succeededPartition);
        partitionIds.add(failedPartition);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.scheduleStepAttempt(succeededPartition);
        history.recordActivityResult(succeededPartition, StepResult.success());

        history.recordScheduleAttemptFailed(failedPartition);


        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assert.assertEquals(1, response.decisions().size());
        Decision decision = response.decisions().get(0);

        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
        Assert.assertEquals(failedPartition, partitionId);

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
        expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(partitionedStep, 0, partitionId);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                expectedInput, activityId);
        attrs = attrs.toBuilder().control(partitionId).build();

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
    }

    @Test
    public void decide_schedulePartitionedSecondStepRetryTimers_OnePartitionFailed_StillScheduleOtherPartition() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String failedPartition = "p1";
        partitionIds.add(failedPartition);
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Set<String> retriedPartitions = new HashSet<>(partitionIds);
        retriedPartitions.remove(failedPartition);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);
            StepResult result = (partitionId.equals(failedPartition) ? StepResult.failure() : StepResult.retry());
            history.recordActivityResult(partitionId, result);
        }

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assert.assertEquals(retriedPartitions.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(retriedPartitions);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assert.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assert.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStepScheduleRetries_AllPartitionsTimersFired() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        Map<String, Instant> partitionFirstAttemptTimes = new HashMap<>();

        for (String partitionId : partitionIds) {
            HistoryEvent startEvent = history.scheduleStepAttempt(partitionId);
            partitionFirstAttemptTimes.put(partitionId, startEvent.eventTimestamp());

            history.recordActivityResult(partitionId, StepResult.retry());
            history.startRetryTimer(partitionId, Duration.ofSeconds(10));
            history.closeRetryTimer(partitionId, false);
        }

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assert.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assert.assertNotNull(partitionId);
            Assert.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.putAll(output);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            expectedInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(1));

            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(partitionFirstAttemptTimes.get(partitionId))));

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                    expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assert.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_scheduleNonPartitionedStepAfterPartitionedStep() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now.minusSeconds(15), input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        HistoryEvent partitionStartEvent = null;
        HistoryEvent partitionEndEvent = null;
        for (String partitionId : partitionIds) {
            HistoryEvent start = history.scheduleStepAttempt(partitionId);
            if (partitionStartEvent == null) {
                partitionStartEvent = start;
            }
            partitionEndEvent = history.recordActivityResult(partitionId, StepResult.success());
        }

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);
        WorkflowStep stepThree = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepThree);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        String partitionedStepName = TaskNaming.activityName(workflowWithPartitionedStepName, partitionedStep);
        Assert.assertNotNull(partitionEndEvent);
        Assert.assertEquals(partitionEndEvent.eventTimestamp().toEpochMilli() - partitionStartEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

        String activityId = TaskNaming.createActivityId(stepThree, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, stepThree,
                expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds_FirstStepHadMultipleAttempts() throws JsonProcessingException {
        Assert.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now.minusSeconds(15), input);

        HistoryEvent firstAttemptBegin = null;
        for (int i = 0; i < 4; i++) {
            HistoryEvent attemptBegin = history.scheduleStepAttempt();
            if (firstAttemptBegin == null) {
                firstAttemptBegin = attemptBegin;
            }
            history.recordActivityResult(StepResult.retry());
            history.startRetryTimer(Duration.ofSeconds(10));
            history.closeRetryTimer(false);
        }

        history.scheduleStepAttempt();
        HistoryEvent lastAttemptEnd = history.recordActivityResult(StepResult.success().withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);

        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        Assert.assertEquals(lastAttemptEnd.eventTimestamp().toEpochMilli() - firstAttemptBegin.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(5, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_scheduleStepWithCustomHeartbeatTimeout() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithCustomHeartbeatTimeout, now, input);
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithCustomHeartbeatTimeout, null, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);
        validateExecutionContext(response.executionContext(), workflowWithCustomHeartbeatTimeout, workflowWithCustomHeartbeatTimeout.getGraph().getFirstStep());

        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assert.assertEquals(
                Long.toString(workflowWithCustomHeartbeatTimeout.getGraph().getFirstStep().activityTaskHeartbeatTimeout().getSeconds()),
                decision.scheduleActivityTaskDecisionAttributes().heartbeatTimeout());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleFirstStepWhenFirstStepNeedsRetry_StartTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);

        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(currentStep, 1, null);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assert.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assert.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_scheduleFirstStep_ScheduleActivityFailedEventForPreviousScheduleAttempt() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.recordScheduleAttemptFailed();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, null, state.getWorkflowId(), state,
                FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        // Start time of the workflow should match what was in the workflow state
        Instant workflowStartTime = StepAttributes.decode(Instant.class, decisionInput.get(StepAttributes.WORKFLOW_START_TIME));
        Assert.assertNotNull(workflowStartTime);
        Assert.assertEquals(state.getWorkflowStartDate(), workflowStartTime);
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(workflowStartTime));

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                input, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleFirstStepWhenFirstStepNeedsRetry_TimerFired() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent startEvent1 = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> stepMetrics);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(startEvent1.eventTimestamp())));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(currentStep, 1, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleStepWhenRetryNowSignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));

        history.recordRetryNowSignal();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assert.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(activityId).build(), decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should always be a hack signal to force a new decision
        Assert.assertEquals(DecisionType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, decisions.get(1).decisionType());
        Assert.assertEquals(DecisionTaskPoller.buildHackSignalDecisionAttrs(state), decisions.get(1).signalExternalWorkflowExecutionDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_IgnoreRetryNowSignalIfNoPreviousTimer_SchedulesNormalRetryTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        history.recordRetryNowSignal();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assert.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assert.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_IgnoreRetryNowSignalIfTimerAlreadyClosed_SchedulesNextActivityAttempt() throws JsonProcessingException {
        Instant now = Instant.now();

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode("workflow-id"));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode("run-id"));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(Date.from(now)));

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent startEvent1 = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        history.recordRetryNowSignal();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(startEvent1.eventTimestamp())));
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, currentStep, input,
                                                                            TaskNaming.createActivityId(currentStep, 1, null));

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_sendScheduleDelayedRetrySignalWhenDelayRetrySignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));

        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assert.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(activityId).build(), decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should to send the workflow a ScheduleDelayedRetry signal.
        Assert.assertEquals(DecisionType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, decisions.get(1).decisionType());
        DelayRetrySignalData signalData = new DelayRetrySignalData();
        signalData.setActivityId(activityId);
        signalData.setDelayInSeconds(142);
        Assert.assertEquals(DecisionTaskPoller.buildScheduleRetrySignalAttrs(state, signalData),
                            decisions.get(1).signalExternalWorkflowExecutionDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_IgnoreDelayRetrySignalIfNoOpenTimer_SchedulesNormalRetryTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assert.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assert.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_IgnoreDelayRetrySignalIfTimerAlreadyClosed_SchedulesNextActivityAttempt() throws JsonProcessingException {
        Instant now = Instant.now();

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode("workflow-id"));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode("run-id"));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(Date.from(now)));

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent startEvent1 = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(startEvent1.eventTimestamp())));
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, currentStep, input,
                                                                            TaskNaming.createActivityId(currentStep, 1, null));

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_createsDelayedRetryTimerWhenScheduleDelayedRetrySignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));
        history.closeRetryTimer(true);
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, 142, null);

        Assert.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assert.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_ignoresScheduleDelayedRetrySignalIfTimerAlreadyOpen() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assert.assertTrue(decisions.toString(), decisions.isEmpty());

        mockery.verify();
    }

    @Test
    public void decide_scheduleNextAttemptAfterDelayedRetryTimerFired() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent startEvent1 = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        history.startRetryTimer(Duration.ofSeconds(10));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));
        history.closeRetryTimer(true);
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));
        history.startRetryTimer(Duration.ofSeconds(142));
        history.closeRetryTimer(false);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });

        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(startEvent1.eventTimestamp())));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));

        String activityIdSecondAttempt = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityIdSecondAttempt);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

    }

    @Test
    public void decide_handleRetryNowForPartitionedStep_OnePartitionCompletedAlready() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String succeededPartition = "p1";
        String retryPendingPartition = "p2";
        partitionIds.add(succeededPartition);
        partitionIds.add(retryPendingPartition);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        history.scheduleStepAttempt(succeededPartition);
        history.scheduleStepAttempt(retryPendingPartition);

        history.recordActivityResult(succeededPartition, StepResult.success());
        history.recordActivityResult(retryPendingPartition, StepResult.retry());

        HistoryEvent timerEvent = history.startRetryTimer(retryPendingPartition, Duration.ofSeconds(10));

        history.recordRetryNowSignal(retryPendingPartition);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assert.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timerEvent.timerStartedEventAttributes().timerId()).build(),
                            decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should always be a hack signal to force a new decision
        Assert.assertEquals(DecisionType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, decisions.get(1).decisionType());
        Assert.assertEquals(DecisionTaskPoller.buildHackSignalDecisionAttrs(state), decisions.get(1).signalExternalWorkflowExecutionDecisionAttributes());
    }

    @Test
    public void decide_ForceResultForPartitionedStep_OnePartitionCompletedAlready() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String succeededPartition = "p1";
        String partitionToForce = "p2";
        partitionIds.add(succeededPartition);
        partitionIds.add(partitionToForce);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        String partitionedStepName = TaskNaming.activityName(workflowWithPartitionedStepName, partitionedStep);
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        HistoryEvent partitionStartEvent = history.scheduleStepAttempt(succeededPartition);
        history.scheduleStepAttempt(partitionToForce);

        history.recordActivityResult(succeededPartition, StepResult.success());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        HistoryEvent timerEvent = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        HistoryEvent partitionEndEvent = history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowGraphNode nextStep = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, nextStep.getStep());
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assert.assertEquals(1, decisions.size());
        Decision decision = decisions.get(0);

        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assert.assertEquals(partitionEndEvent.eventTimestamp().toEpochMilli() - partitionStartEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

        String activityId = TaskNaming.createActivityId(nextStep.getStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, nextStep.getStep(),
                                                                            expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
    }

    @Test
    public void decide_ForceResultForPartitionedStep_OtherPartitionCompletedAfterSignal() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String retryingPartition = "p1";
        String partitionToForce = "p2";
        partitionIds.add(retryingPartition);
        partitionIds.add(partitionToForce);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        String partitionedStepName = TaskNaming.activityName(workflowWithPartitionedStepName, partitionedStep);
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        HistoryEvent partitionStartEvent = history.scheduleStepAttempt(retryingPartition);
        history.scheduleStepAttempt(partitionToForce);

        history.recordActivityResult(retryingPartition, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        history.startRetryTimer(retryingPartition, Duration.ofSeconds(10));
        history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);
        history.closeRetryTimer(partitionToForce, true);

        history.closeRetryTimer(retryingPartition, false);

        history.scheduleStepAttempt(retryingPartition);
        HistoryEvent partitionEndEvent = history.recordActivityResult(retryingPartition, StepResult.success());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        WorkflowGraphNode nextStep = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, nextStep.getStep());
        mockery.verify();

        Assert.assertEquals(1, response.decisions().size());

        Decision decision = response.decisions().get(0);

        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assert.assertEquals(partitionEndEvent.eventTimestamp().toEpochMilli() - partitionStartEvent.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)).toMillis());
        Assert.assertEquals(2, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        // because this is the first attempt of the step and we don't control 'now' from the unit test,
        // the ACTIVITY_INITIAL_ATTEMPT_TIME will vary from test to test.
        // to solve this we'll extract ACTIVITY_INTIAL_ATTEMPT_TIME, ensure it's not null and a valid Date,
        // and then save it to the input map for the actual comparison.
        Map<String, String> decisionInput = StepAttributes.decode(Map.class, decision.scheduleActivityTaskDecisionAttributes().input());
        Date attemptTime = StepAttributes.decode(Date.class, decisionInput.get(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        Assert.assertNotNull(attemptTime);
        Assert.assertTrue(now.minusSeconds(30).isBefore(attemptTime.toInstant()));
        Assert.assertTrue(now.plusSeconds(30).isAfter(attemptTime.toInstant()));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(attemptTime));

        String activityId = TaskNaming.createActivityId(nextStep.getStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, nextStep.getStep(),
                expectedInput, activityId);

        Assert.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
    }

    @Test
    public void decide_ForceResultForPartitionedStep_OtherPartitionStillPendingRetry() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String retryingPartition = "p1";
        String partitionToForce = "p2";
        partitionIds.add(retryingPartition);
        partitionIds.add(partitionToForce);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        history.scheduleStepAttempt(retryingPartition);
        history.scheduleStepAttempt(partitionToForce);

        history.recordActivityResult(retryingPartition, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        history.startRetryTimer(retryingPartition, Duration.ofSeconds(10));
        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        Decision decision = response.decisions().get(0);
        mockery.verify();

        // only decision should be to cancel the current timer
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
                            decision.cancelTimerDecisionAttributes());

        // no other decisions, because we're still waiting to retry the other partition.
    }

    @Test
    public void decide_ForceResultForPartitionedStep_OtherPartitionNeedsTimerScheduled() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String partitionToReschedule = "p1";
        String partitionToForce = "p2";
        partitionIds.add(partitionToReschedule);
        partitionIds.add(partitionToForce);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        history.scheduleStepAttempt(partitionToReschedule);
        history.scheduleStepAttempt(partitionToForce);

        history.recordActivityResult(partitionToReschedule, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assert.assertEquals(2, decisions.size());

        // first decision should be to schedule the retry timer for partition p1
        Assert.assertEquals(DecisionType.START_TIMER, decisions.get(0).decisionType());

        String activityP1Id = TaskNaming.createActivityId(partitionedStep, 1, partitionToReschedule);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(partitionedStep, 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityP1Id, delayInSeconds, null);

        Assert.assertEquals(attrs.timerId(), decisions.get(0).startTimerDecisionAttributes().timerId());
        Assert.assertNotNull(decisions.get(0).startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertTrue(0 < Integer.parseInt(decisions.get(0).startTimerDecisionAttributes().startToFireTimeout()));

        // second decision should be to cancel the p2 timer
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(1).decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
                            decisions.get(1).cancelTimerDecisionAttributes());
    }

    @Test
    public void decide_ForceResultForPartitionedStep_OtherPartitionNeedsRetryScheduled() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String partitionToReschedule = "p1";
        String partitionToForce = "p2";
        partitionIds.add(partitionToReschedule);
        partitionIds.add(partitionToForce);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Instant now = Instant.now();

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode("workflow-id"));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode("run-id"));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(Date.from(now)));

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, now, input);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success().withAttributes(output));

        HistoryEvent startP1Event = history.scheduleStepAttempt(partitionToReschedule);
        history.scheduleStepAttempt(partitionToForce);

        history.recordActivityResult(partitionToReschedule, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        history.startRetryTimer(partitionToReschedule, Duration.ofSeconds(10));
        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        history.closeRetryTimer(partitionToReschedule, false);
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assert.assertEquals(2, decisions.size());

        // first decision should be to reschedule partition p1
        Assert.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decisions.get(0).decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionToReschedule));
        expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
        expectedInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(1));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(Date.from(startP1Event.eventTimestamp())));

        String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionToReschedule);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                                                                            expectedInput, activityId);
        attrs = attrs.toBuilder().control(partitionToReschedule).build();

        Assert.assertEquals(attrs, decisions.get(0).scheduleActivityTaskDecisionAttributes());

        // second decision should be to cancel the p2 timer
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(1).decisionType());
        Assert.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
                            decisions.get(1).cancelTimerDecisionAttributes());
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_PreviousStepAlreadySucceeded() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now);

        HistoryEvent start1Event = history.scheduleStepAttempt();
        HistoryEvent close1Event = history.recordActivityResult(StepResult.success());

        HistoryEvent cancelRequestEvent = history.recordCancelWorkflowExecutionRequest();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assert.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assert.assertEquals(close1Event.eventTimestamp().toEpochMilli() - start1Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)).toMillis());
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)).toMillis());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelsInProgressActivity() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now);

        HistoryEvent start1Event = history.scheduleStepAttempt();
        HistoryEvent cancelRequestEvent = history.recordCancelWorkflowExecutionRequest();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepOne, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, null);

        Assert.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.REQUEST_CANCEL_ACTIVITY_TASK, decision.decisionType());

        RequestCancelActivityTaskDecisionAttributes activityAttrs = RequestCancelActivityTaskDecisionAttributes.builder()
                .activityId(TaskNaming.createActivityId(stepOne, 0, null)).build();
        Assert.assertEquals(activityAttrs, decision.requestCancelActivityTaskDecisionAttributes());

        decision = response.decisions().get(1);
        Assert.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assert.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - start1Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)).toMillis());
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)).toMillis());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelsOpenRetryTimer() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now);

        HistoryEvent start1Event = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timer1Event = history.startRetryTimer(Duration.ofSeconds(10));

        HistoryEvent cancelRequestEvent = history.recordCancelWorkflowExecutionRequest();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepOne, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, null);

        Assert.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());

        CancelTimerDecisionAttributes timerAttrs = CancelTimerDecisionAttributes.builder()
                .timerId(timer1Event.timerStartedEventAttributes().timerId())
                .build();
        Assert.assertEquals(timerAttrs, decision.cancelTimerDecisionAttributes());

        decision = response.decisions().get(1);
        Assert.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assert.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - start1Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)).toMillis());
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)).toMillis());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelIsHigherPriorityThanSignal() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now);

        HistoryEvent start1Event = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timer1Event = history.startRetryTimer(Duration.ofSeconds(10));

        HistoryEvent cancelRequestEvent = history.recordCancelWorkflowExecutionRequest();

        history.recordRetryNowSignal();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, null);

        Assert.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());

        CancelTimerDecisionAttributes timerAttrs = CancelTimerDecisionAttributes.builder()
                .timerId(timer1Event.timerStartedEventAttributes().timerId())
                .build();
        Assert.assertEquals(timerAttrs, decision.cancelTimerDecisionAttributes());

        decision = response.decisions().get(1);
        Assert.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assert.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String activityName = TaskNaming.activityName(workflowName, currentStep);
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - start1Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(cancelRequestEvent.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)).toMillis());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_completesWorkflowWhenLastStepSucceeds() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent start2Event = history.scheduleStepAttempt();
        HistoryEvent close2Event = history.recordActivityResult(StepResult.success());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflow, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.COMPLETE_WORKFLOW_EXECUTION, decision.decisionType());

        CompleteWorkflowExecutionDecisionAttributes attrs = CompleteWorkflowExecutionDecisionAttributes.builder().build();
        Assert.assertEquals(attrs, decision.completeWorkflowExecutionDecisionAttributes());

        String stepTwoActivityName = TaskNaming.activityName(workflowName, stepTwo);
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)).toMillis());
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - start2Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_failsWorkflowWhenStepWithCloseOnFailureTransitionFails() throws JsonProcessingException {
        Workflow workflowWithFailureTransition = new TestWorkflowWithFailureTransition();
        String workflowWithFailureTransitionName = TaskNaming.workflowName(workflowWithFailureTransition);

        WorkflowStep currentStep = workflowWithFailureTransition.getGraph().getFirstStep();
        String activityName = TaskNaming.activityName(workflowWithFailureTransitionName, currentStep);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithFailureTransition, now);

        HistoryEvent start1Event = history.scheduleStepAttempt();
        HistoryEvent close1Event = history.recordActivityResult(StepResult.failure());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithFailureTransition, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), workflowWithFailureTransition, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.FAIL_WORKFLOW_EXECUTION, decision.decisionType());

        Assert.assertNotNull(decision.failWorkflowExecutionDecisionAttributes().reason());
        Assert.assertTrue(decision.failWorkflowExecutionDecisionAttributes().reason().startsWith(activityName + " failed after"));
        Assert.assertNotNull("FailWorkflowExecutionDecision detail should not be null!", decision.failWorkflowExecutionDecisionAttributes().details());

        Assert.assertEquals(close1Event.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowWithFailureTransitionName)).toMillis());
        Assert.assertEquals(close1Event.eventTimestamp().toEpochMilli() - start1Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_periodicWorkflowSchedulesDelayExitTimerWhenLastStepSucceeds() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, now);

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent start2Event = history.scheduleStepAttempt();
        HistoryEvent close2Event = history.recordActivityResult(StepResult.success());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        Assert.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, decision.startTimerDecisionAttributes().timerId());

        mockery.verify();

        // we want to see a workflow execution time metric emitted when the periodic workflow ends, before the delayed exit
        String stepTwoActivityName = TaskNaming.activityName(periodicWorkflowName, stepTwo);
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)).toMillis());
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - start2Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        Periodic p = periodicWorkflow.getClass().getAnnotation(Periodic.class);

        // the timer should be set to end such that the workflow will be run once per period;
        // so the actual delay scheduled should be within a few seconds of the remaining time.
        long timeSinceWorkflowStart = now.toEpochMilli() - state.getWorkflowStartDate().toEpochMilli();
        long expectedDelay = p.intervalUnits().toSeconds(p.runInterval()) - (timeSinceWorkflowStart/1000);
        long allowedDelta = 2;
        // start-to-fire timeout is in seconds
        long actualDelay = Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertEquals(expectedDelay, actualDelay, allowedDelta);
    }

    /**
     * This test exists because Flux used to use joda's Duration to calculate durations, but joda's Duration
     * throws an exception if your duration is more than 30 days. java 8's Duration behaves sanely.
     */
    @Test
    public void decide_periodicWorkflowSchedulesDelayExitTimerWhenLastStepSucceeds_StepTookMoreThan30Days() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, now.minus(Duration.ofDays(45)));

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent start2Event = history.scheduleStepAttempt();
        HistoryEvent close2Event = history.recordActivityResult(StepResult.success());

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        Assert.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, decision.startTimerDecisionAttributes().timerId());

        mockery.verify();

        // we want to see a workflow execution time metric emitted when the periodic workflow ends, before the delayed exit
        String stepTwoActivityName = TaskNaming.activityName(periodicWorkflowName, stepTwo);
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - state.getWorkflowStartDate().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)).toMillis());
        Assert.assertEquals(close2Event.eventTimestamp().toEpochMilli() - start2Event.eventTimestamp().toEpochMilli(),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)).toMillis());
        Assert.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assert.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assert.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        // the timer should be set to 1 second since the workflow ran for 45 days and our minimum delay time is 1 second.
        long actualDelay = Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assert.assertEquals(1.0, actualDelay, 0.0);
    }

    @Test
    public void decide_periodicWorkflowContinuesAsNewWorkflowWhenDelayExitTimerFires() throws JsonProcessingException {
        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, now.minus(Duration.ofDays(45)));

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.startDelayExitTimer();
        history.closeDelayExitTimer(false);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION, decision.decisionType());

        ContinueAsNewWorkflowExecutionDecisionAttributes attrs = ContinueAsNewWorkflowExecutionDecisionAttributes.builder()
                .childPolicy(ChildPolicy.TERMINATE)
                .input(StepAttributes.encode(Collections.emptyMap()))
                .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                .executionStartToCloseTimeout(Long.toString(periodicWorkflow.maxStartToCloseDuration().getSeconds()))
                .taskList(TaskList.builder().name(periodicWorkflow.taskList()).build())
                .build();
        Assert.assertEquals(attrs, decision.continueAsNewWorkflowExecutionDecisionAttributes());

        // we do *not* want to see a workflow execution time metric emitted after the delayed exit ends
        Assert.assertFalse(deciderMetrics.getDurations().containsKey(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)));

        mockery.verify();
    }

    @Test
    public void decide_periodicWorkflowContinuesAsNewWorkflowWhenDelayExitTimerFires_CustomTaskList() throws JsonProcessingException {
        Workflow periodicCustomTaskList = new TestPeriodicWorkflowCustomTaskList();
        String periodicCustomTaskListWorkflowName = TaskNaming.workflowName(periodicCustomTaskList);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicCustomTaskList, now.minus(Duration.ofDays(45)));

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.startDelayExitTimer();
        history.closeDelayExitTimer(false);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicCustomTaskList.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicCustomTaskList, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics, (o) -> { throw new RuntimeException("shouldn't request stepMetrics"); });
        validateExecutionContext(response.executionContext(), periodicCustomTaskList, null);
        Decision decision = response.decisions().get(0);
        Assert.assertEquals(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION, decision.decisionType());

        ContinueAsNewWorkflowExecutionDecisionAttributes attrs = ContinueAsNewWorkflowExecutionDecisionAttributes.builder()
                .childPolicy(ChildPolicy.TERMINATE)
                .input(StepAttributes.encode(Collections.emptyMap()))
                .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                .executionStartToCloseTimeout(Long.toString(periodicCustomTaskList.maxStartToCloseDuration().getSeconds()))
                .taskList(TaskList.builder().name(periodicCustomTaskList.taskList()).build())
                .build();
        Assert.assertEquals(attrs, decision.continueAsNewWorkflowExecutionDecisionAttributes());

        // we do *not* want to see a workflow execution time metric emitted after the delayed exit ends
        Assert.assertFalse(deciderMetrics.getDurations().containsKey(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicCustomTaskListWorkflowName)));

        mockery.verify();
    }

    @Test
    public void findNextStep_FindsFirstStepWhenNoCurrentStep() {
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, null, null);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(workflow.getGraph().getFirstStep(), selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStepWhenCurrentIsFirstStep() {
        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(stepTwo, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_BranchingWorkflow_RespectsDefaultResultCode() {
        Workflow branchingWorkflow = new TestBranchingWorkflow();

        WorkflowStep stepOne = branchingWorkflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = branchingWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(branchingWorkflow, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(stepTwo, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_BranchingWorkflow_RespectsCustomResultCode() {
        Workflow branchingWorkflow = new TestBranchingWorkflow();

        WorkflowStep stepOne = branchingWorkflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep branchStep = branchingWorkflow.getGraph().getNodes().get(TestBranchStep.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(branchingWorkflow, stepOne, TestBranchingWorkflow.CUSTOM_RESULT);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(branchStep, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_NoNextStepWhenCurrentStepIsLast() {
        WorkflowStep secondStep = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, secondStep, StepResult.SUCCEED_RESULT_CODE);
        Assert.assertTrue(selection.workflowShouldClose());
        Assert.assertNull(selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeSucceed() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(stepTwo, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeFail() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, StepResult.FAIL_RESULT_CODE);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(stepTwo, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeCustom() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, "some-random-custom-code");
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertEquals(stepTwo, selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeSucceed() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, StepResult.SUCCEED_RESULT_CODE);
        Assert.assertTrue(selection.workflowShouldClose());
        Assert.assertNull(selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeFail() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, StepResult.FAIL_RESULT_CODE);
        Assert.assertTrue(selection.workflowShouldClose());
        Assert.assertNull(selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeCustom() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, "some-random-custom-code");
        Assert.assertTrue(selection.workflowShouldClose());
        Assert.assertNull(selection.getNextStep());
        Assert.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_UnknownResultCode() {
        TestWorkflowDoesntHandleCustomResultCode wf = new TestWorkflowDoesntHandleCustomResultCode();
        WorkflowStep stepOne = wf.getGraph().getNodes().get(TestStepReturnsCustomResultCode.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(wf, stepOne, TestStepReturnsCustomResultCode.RESULT_CODE);
        Assert.assertFalse(selection.workflowShouldClose());
        Assert.assertNull(selection.getNextStep());
        Assert.assertTrue(selection.isNextStepUnknown());
    }

    private void expectPoll(PollForDecisionTaskResponse taskToReturn) {
        expectPoll(taskToReturn, Workflow.DEFAULT_TASK_LIST_NAME);
    }

    private void expectPoll(PollForDecisionTaskResponse taskToReturn, String taskList) {
        PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder().domain(DOMAIN)
                .taskList(TaskList.builder().name(taskList).build()).identity(IDENTITY).reverseOrder(true).build();
        EasyMock.expect(swf.pollForDecisionTask(request)).andReturn(taskToReturn);
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomInitialRetryDelay() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 300, maxRetryDelaySeconds = 3000, jitterPercent = 0)
            public void apply() {}
        };

        // since jitterPercent is 0 and the initial retry delay is 300, the first retry should use 300 seconds
        Assert.assertEquals(300, RetryUtils.calculateRetryBackoffInSeconds(step, 1, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomMaxRetryDelay() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 10, maxRetryDelaySeconds = 20, jitterPercent = 0)
            public void apply() {}
        };

        // since jitterPercent is 0 and the max retry delay is 20, the 10000th retry should use 20 seconds
        Assert.assertEquals(20, RetryUtils.calculateRetryBackoffInSeconds(step, 10000, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomBackoffBase() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 10, retriesBeforeBackoff = 6, jitterPercent = 0, exponentialBackoffBase = 1.25)
            public void apply() {}
        };

        // since jitterPercent is 0 the first 6 retries should use 10 seconds, then attempt 7 should use 12 seconds (10 * (1.25 ^ 1)),
        // and attempt 8 should use 15 seconds (10 * (1.25 ^ 2))
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 1, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 2, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 3, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 4, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 5, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 6, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(12, RetryUtils.calculateRetryBackoffInSeconds(step, 7, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assert.assertEquals(15, RetryUtils.calculateRetryBackoffInSeconds(step, 8, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
    }
    @Test
    public void testCalculateRetryBackoff_RespectsDefaultJitterPercent() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 100, maxRetryDelaySeconds = 100)
            public void apply() {}
        };

        // the default jitter percent is 10, so we should see no retry times < 90 or > 110
        for (int attempt = 1; attempt <= 1000; attempt++) {
            long calculatedTime = RetryUtils.calculateRetryBackoffInSeconds(step, attempt, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
            Assert.assertTrue(calculatedTime >= 90 && calculatedTime <= 110);
        }
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomJitterPercent() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 100, maxRetryDelaySeconds = 100, jitterPercent = 90)
            public void apply() {}
        };

        boolean foundLargerJitter = false;
        // The custom jitter percent is 90, so we should see no retry times < 10 or > 190,
        // and we should see at least one retry time < 90 or > 110 given a large enough sample size.
        // Technically this could fail even if the code works properly but the likelihood is pretty small
        // given the jitter percent of 90 and 1000 attempts.
        for (int attempt = 1; attempt <= 1000; attempt++) {
            long calculatedTime = RetryUtils.calculateRetryBackoffInSeconds(step, attempt, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
            Assert.assertTrue(calculatedTime >= 10 && calculatedTime <= 190);
            if (calculatedTime < 90 || calculatedTime > 110) {
                foundLargerJitter = true;
            }
        }
        Assert.assertTrue(foundLargerJitter);
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomRetriesBeforeBackoff() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 300, maxRetryDelaySeconds = 3000, jitterPercent = 0, retriesBeforeBackoff = 27)
            public void apply() {}
        };

        // since jitterPercent is 0 and the initial retry delay is 300, the first 27 retries should use 300 seconds
        for (int attempt = 1; attempt <= 27; attempt++) {
            Assert.assertEquals(300, RetryUtils.calculateRetryBackoffInSeconds(step, attempt, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        }
        // the 28th retry should delay longer than 300 seconds
        Assert.assertTrue(RetryUtils.calculateRetryBackoffInSeconds(step, 28, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE) > 300);
    }

    private void validateExecutionContext(String executionContext, Workflow workflow, WorkflowStep expectedNextStep) {
        if (expectedNextStep == null) {
            if (workflow.getClass().isAnnotationPresent(Periodic.class)) {
                Assert.assertNotNull(executionContext);
                Map<String, String> decodedExecutionContext = StepAttributes.decode(Map.class, executionContext);
                Assert.assertNotNull(decodedExecutionContext);
                Assert.assertEquals(1, decodedExecutionContext.size());
                Assert.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, StepAttributes.decode(String.class, decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_NAME)));
            } else {
                Assert.assertNull(executionContext);
            }
        } else {
            Assert.assertNotNull(executionContext);
            Map<String, String> decodedExecutionContext = StepAttributes.decode(Map.class, executionContext);
            Assert.assertNotNull(decodedExecutionContext);
            Assert.assertEquals(2, decodedExecutionContext.size());
            Assert.assertEquals(expectedNextStep.getClass().getSimpleName(), StepAttributes.decode(String.class, decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_NAME)));

            String encodedResultCodes = decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_RESULT_CODES);
            Assert.assertNotNull(encodedResultCodes);
            Map<String, String> decodedResultCodeMap = StepAttributes.decode(Map.class, encodedResultCodes);
            Assert.assertNotNull(decodedResultCodeMap);
            Assert.assertEquals(workflow.getGraph().getNodes().get(expectedNextStep.getClass()).getNextStepsByResultCode().size(), decodedResultCodeMap.size());
            for (Map.Entry<String, WorkflowGraphNode> resultCodes : workflow.getGraph().getNodes().get(expectedNextStep.getClass()).getNextStepsByResultCode().entrySet()) {
                Assert.assertTrue(decodedResultCodeMap.containsKey(resultCodes.getKey()));
                if (resultCodes.getValue() == null) {
                    Assert.assertEquals(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_RESULT_WORKFLOW_ENDS, decodedResultCodeMap.get(resultCodes.getKey()));
                } else {
                    Assert.assertEquals(resultCodes.getValue().getStep().getClass().getSimpleName(), decodedResultCodeMap.get(resultCodes.getKey()));
                }
            }
        }
    }
}
