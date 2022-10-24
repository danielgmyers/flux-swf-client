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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.clients.swf.IdUtils;
import com.danielgmyers.flux.clients.swf.poller.signals.DelayRetrySignalData;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestBranchStep;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestBranchingWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPartitionedStep;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPeriodicWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPeriodicWorkflowCustomTaskList;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepHasOptionalInputAttribute;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepOne;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepReturnsCustomResultCode;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepTwo;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowCustomTaskList;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowDoesntHandleCustomResultCode;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithAlwaysTransition;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithFailureTransition;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithPartitionedStep;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithStepCustomHeartbeatTimeout;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.StepResult;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.util.RetryUtils;
import com.danielgmyers.flux.clients.swf.util.ManualClock;
import com.danielgmyers.flux.clients.swf.wf.Periodic;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private ManualClock clock;

    @BeforeEach
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
            public MetricRecorder newMetricRecorder(String operation, Clock clock) {
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

        clock = new ManualClock();

        executor = new BlockOnSubmissionThreadPoolExecutor(1, "executor");
        poller = new DecisionTaskPoller(metricsFactory, swf, DOMAIN, Workflow.DEFAULT_TASK_LIST_NAME, IDENTITY,
                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE, workflows, activities,
                                        executor, clock);
    }

    @Test
    public void doesNothingIfNoWorkObjectReturned() throws InterruptedException {
        expectPoll(null);
        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(stepMetricsRequested);
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
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(deciderMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
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
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(deciderMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
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

        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(deciderMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
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

        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(deciderMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
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

        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertEquals(1, pollMetrics.getCounts().get(DecisionTaskPoller.NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME).longValue());
        Assertions.assertTrue(pollMetrics.isClosed());

        Assertions.assertFalse(deciderMetricsRequested);
        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void testRun_SubmitsDecisionResult() throws InterruptedException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);
        WorkflowState state = history.buildCurrentState();
        expectPoll(history.buildDecisionTask());

        // for the purposes of this test we don't care what the actual request contains
        EasyMock.expect(swf.respondDecisionTaskCompleted(EasyMock.isA(RespondDecisionTaskCompletedRequest.class)))
                .andReturn(RespondDecisionTaskCompletedResponse.builder().build());

        mockery.replay();
        poller.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + Workflow.DEFAULT_TASK_LIST_NAME));
        Assertions.assertTrue(pollMetrics.isClosed());
        Assertions.assertTrue(deciderMetricsRequested);
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatDecisionTaskEventHistoryPageCountMetricName(workflowName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(deciderMetrics.isClosed());

        Assertions.assertFalse(stepMetricsRequested);
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
                                                                         workflows, activities, executor, clock);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithCustomTaskList, clock);
        WorkflowState state = history.buildCurrentState();
        expectPoll(history.buildDecisionTask(), workflowWithCustomTaskList.taskList());

        // for the purposes of this test we don't care what the actual request contains
        EasyMock.expect(swf.respondDecisionTaskCompleted(EasyMock.isA(RespondDecisionTaskCompletedRequest.class)))
                .andReturn(RespondDecisionTaskCompletedResponse.builder().build());

        mockery.replay();
        pollerCustomTaskList.run();
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECISION_TASK_POLL_TIME_METRIC_PREFIX + "Time"));
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME));
        Assertions.assertNotNull(pollMetrics.getDurations().get(DecisionTaskPoller.DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + customTaskListName));
        Assertions.assertTrue(pollMetrics.isClosed());
        Assertions.assertTrue(deciderMetricsRequested);
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatDecisionTaskEventHistoryPageCountMetricName(workflowWithCustomTaskListName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(deciderMetrics.isClosed());

        Assertions.assertFalse(stepMetricsRequested);
        mockery.verify();
    }

    @Test
    public void decide_scheduleFirstStepWhenNoCurrentStep() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        // move the clock forward so that when the decision is made,
        // the first step's initial attempt time will be different than the workflow start time
        Instant activityInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, null, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics, (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, workflow.getGraph().getFirstStep());
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(activityInitialAttemptTime));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success().withAttributes(output));

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Map<String, String> expectedInput = new TreeMap<>();
        expectedInput.putAll(input);
        expectedInput.putAll(output);

        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_recordMarkerAndRetryTimerIfUnknownResultCode() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.complete("custom-result-code", "Graph doesn't handle this result code").withAttributes(output));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(2, response.decisions().size());
        Decision markerDecision = response.decisions().get(0);
        Decision timerDecision = response.decisions().get(1);

        Assertions.assertEquals(DecisionType.RECORD_MARKER, markerDecision.decisionType());
        Assertions.assertEquals(DecisionType.START_TIMER, timerDecision.decisionType());

        StartTimerDecisionAttributes timerAttrs = timerDecision.startTimerDecisionAttributes();
        Assertions.assertEquals(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_ID, timerAttrs.timerId());
        Assertions.assertEquals(Long.toString(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_DELAY.getSeconds()),
                            timerAttrs.startToFireTimeout());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.UNKNOWN_RESULT_CODE_METRIC_BASE).longValue());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowMetricName(workflowName)).longValue());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowStepMetricName(activityName)).longValue());

        mockery.verify();
    }

    @Test
    public void decide_recordMarkerAndRetryTimerIfUnknownResultCode_DoNotCreateDuplicateTimer() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.complete("custom-result-code", "Graph doesn't handle this result code").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        history.startTimer(DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_ID,
                           DecisionTaskPoller.UNKNOWN_RESULT_RETRY_TIMER_DELAY);

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(1, response.decisions().size());
        Decision markerDecision = response.decisions().get(0);

        Assertions.assertEquals(DecisionType.RECORD_MARKER, markerDecision.decisionType());

        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.UNKNOWN_RESULT_CODE_METRIC_BASE).longValue());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowMetricName(workflowName)).longValue());
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatUnknownResultCodeWorkflowStepMetricName(activityName)).longValue());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds_PreviousScheduleAttemptFailed() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        WorkflowStep firstStep = workflow.getGraph().getFirstStep();
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        history.recordScheduleAttemptFailed();

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, firstStep, state.getWorkflowId(), state,
                FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);

        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>();
        expectedInput.putAll(input);
        expectedInput.putAll(output);
        expectedInput.remove(StepAttributes.RESULT_CODE);

        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepResultCodeForcedBySignal() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        HistoryEvent startEvent = history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        Instant forceResultSignalTime = clock.forward(Duration.ofMillis(100));
        history.recordForceResultSignal(StepResult.SUCCEED_RESULT_CODE);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();

        // we expect two decisions: one for scheduling the next step, and one for canceling the retry timer for the current step.
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        List<Decision> decisions = response.decisions();
        Assertions.assertEquals(1, decisions.size());
        Decision decision = decisions.get(0);

        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);

        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        // if we scheduled the second step via force-result, then step one's execution time should be
        // from the beginning of its first attempt until the time of the force-result signal.
        Assertions.assertEquals(Duration.between(startEvent.eventTimestamp(), forceResultSignalTime),
                            deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_schedulePartitionedSecondStep_OnlyOnePartition_ScheduleActivityFailed() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String failedPartition = "p1";
        partitionIds.add(failedPartition);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.recordScheduleAttemptFailed(failedPartition);

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, currentStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(partitionIds.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        // there should be two decisions in the response: a metadata marker, and a signal.
        Assertions.assertEquals(2, response.decisions().size());
        Assertions.assertEquals(DecisionType.RECORD_MARKER, response.decisions().get(0).decisionType());

        PartitionIdGeneratorResult expectedPartitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));
        PartitionMetadata expectedPartitionMetadata
                = PartitionMetadata.fromPartitionIdGeneratorResult(expectedPartitionIdGeneratorResult);

        Assertions.assertEquals(TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(TestPartitionedStep.class), 0, 1),
                            response.decisions().get(0).recordMarkerDecisionAttributes().markerName());
        Assertions.assertEquals(expectedPartitionMetadata.toMarkerDetailsList().get(0),
                            response.decisions().get(0).recordMarkerDecisionAttributes().details());

        // second decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(1));
    }

    @Test
    public void decide_schedulePartitionedSecondStep_TwoPartitions_ScheduleActivityFailed() throws JsonProcessingException {
        List<String> failedPartitions = new ArrayList<>();
        failedPartitions.add("p1");
        failedPartitions.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(failedPartitions);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));

        for (String partitionId : failedPartitions) {
            history.recordScheduleAttemptFailed(partitionId);
        }

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, currentStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(failedPartitions.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        // there should be two decisions in the response: a metadata marker, and a signal.
        Assertions.assertEquals(2, response.decisions().size());
        Assertions.assertEquals(DecisionType.RECORD_MARKER, response.decisions().get(0).decisionType());

        PartitionIdGeneratorResult expectedPartitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(failedPartitions));
        PartitionMetadata expectedPartitionMetadata
                = PartitionMetadata.fromPartitionIdGeneratorResult(expectedPartitionIdGeneratorResult);

        Assertions.assertEquals(TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(TestPartitionedStep.class), 0, 1),
                            response.decisions().get(0).recordMarkerDecisionAttributes().markerName());
        Assertions.assertEquals(expectedPartitionMetadata.toMarkerDetailsList().get(0),
                            response.decisions().get(0).recordMarkerDecisionAttributes().details());

        // second decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(1));
    }

    @Test
    public void decide_schedulePartitionedSecondStep_TwoPartitions_ScheduleActivityFailed_MetadataMarkerAlreadyPresent() throws JsonProcessingException {
        List<String> failedPartitions = new ArrayList<>();
        failedPartitions.add("p1");
        failedPartitions.add("p2");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(failedPartitions);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));

        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(failedPartitions));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        clock.forward(Duration.ofMillis(100));

        for (String partitionId : failedPartitions) {
            history.recordScheduleAttemptFailed(partitionId);
        }

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,
                 (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, currentStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();

        Assertions.assertEquals(failedPartitions.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(failedPartitions);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(failedPartitions.size()));

            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));

            String activityId = TaskNaming.createActivityId(stepTwo, 0, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, stepTwo,
                                                                                expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds_NoMarkerYet() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = workflowWithPartitionedStep.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, firstStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(partitionIds.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        // there should be two decisions in the response: a metadata marker, and a signal.
        Assertions.assertEquals(2, response.decisions().size());
        Assertions.assertEquals(DecisionType.RECORD_MARKER, response.decisions().get(0).decisionType());

        PartitionIdGeneratorResult expectedPartitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));
        PartitionMetadata expectedPartitionMetadata
                = PartitionMetadata.fromPartitionIdGeneratorResult(expectedPartitionIdGeneratorResult);

        Assertions.assertEquals(TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(TestPartitionedStep.class), 0, 1),
                            response.decisions().get(0).recordMarkerDecisionAttributes().markerName());
        Assertions.assertEquals(expectedPartitionMetadata.toMarkerDetailsList().get(0),
                            response.decisions().get(0).recordMarkerDecisionAttributes().details());

        // second decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(1));
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds_MultipleMarkersNeeded() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = workflowWithPartitionedStep.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, firstStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(partitionIds.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        PartitionIdGeneratorResult expectedPartitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));
        PartitionMetadata expectedPartitionMetadata
                = PartitionMetadata.fromPartitionIdGeneratorResult(expectedPartitionIdGeneratorResult);
        List<String> markerDetailsList = expectedPartitionMetadata.toMarkerDetailsList();

        // There should be two kinds of decisions in the response: metadata markers, and a signal.
        Assertions.assertEquals(1 + markerDetailsList.size(), response.decisions().size());

        for (int i = 0; i < markerDetailsList.size(); i++) {
            Assertions.assertEquals(DecisionType.RECORD_MARKER, response.decisions().get(i).decisionType());

            Assertions.assertEquals(TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(TestPartitionedStep.class), i, markerDetailsList.size()),
                                response.decisions().get(i).recordMarkerDecisionAttributes().markerName());
            Assertions.assertEquals(markerDetailsList.get(i),
                                response.decisions().get(i).recordMarkerDecisionAttributes().details());
        }

        // The last decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(markerDetailsList.size()));
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds_MultipleMarkersNeeded_IgnoresExistingPartialMarkerSet() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));

        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(partitionIdGeneratorResult);
        List<String> markerDetailsList = metadata.toMarkerDetailsList();

        // Note we're intentionally omitting the last marker, to simulate a scenario where somehow one of the markers didn't make it into the history.
        // We add them all at once so SWF will add them all at the same time, so this shouldn't happen unless there's a bug in Flux.
        for (int i = 0; i < markerDetailsList.size() - 1; i++) {
            history.recordMarker(clock.instant(), TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(partitionedStep), i, markerDetailsList.size()),
                                     markerDetailsList.get(i));
        }

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = workflowWithPartitionedStep.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, firstStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(partitionIds.size(), stepMetrics.getCounts().get(TestPartitionedStep.PARTITION_ID_GENERATOR_METRIC).intValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.isClosed());

        mockery.verify();

        PartitionIdGeneratorResult expectedPartitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));
        PartitionMetadata expectedPartitionMetadata
                = PartitionMetadata.fromPartitionIdGeneratorResult(expectedPartitionIdGeneratorResult);
        List<String> expectedMarkerDetailsList = expectedPartitionMetadata.toMarkerDetailsList();

        // There should be two kinds of decisions in the response: metadata markers, and a signal.
        Assertions.assertEquals(1 + expectedMarkerDetailsList.size(), response.decisions().size());

        for (int i = 0; i < expectedMarkerDetailsList.size(); i++) {
            Assertions.assertEquals(DecisionType.RECORD_MARKER, response.decisions().get(i).decisionType());

            Assertions.assertEquals(TaskNaming.partitionMetadataMarkerName(TaskNaming.stepName(TestPartitionedStep.class), i, expectedMarkerDetailsList.size()),
                                response.decisions().get(i).recordMarkerDecisionAttributes().markerName());
            Assertions.assertEquals(expectedMarkerDetailsList.get(i),
                                response.decisions().get(i).recordMarkerDecisionAttributes().details());
        }

        // The last decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(markerDetailsList.size()));
    }

    @Test
    public void decide_schedulePartitionedSecondStepWhenFirstStepSucceeds_MarkerAlreadyPresent() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep firstStep = workflowWithPartitionedStep.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, firstStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String activityName = TaskNaming.activityName(workflowWithPartitionedStepName, firstStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());

        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();

        Assertions.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            expectedInput.putAll(output);

            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));

            String activityId = TaskNaming.createActivityId(stepTwo, 0, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, stepTwo,
                                                                                expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        clock.forward(Duration.ofMillis(100));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);

            clock.forward(Duration.ofMillis(100));

            history.recordActivityResult(partitionId, StepResult.retry());

            clock.rewind(Duration.ofMillis(100));
        }

        // move back ahead of the partitions
        clock.forward(Duration.ofMillis(150));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assertions.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assertions.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assertions.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success("finished!").withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        clock.forward(Duration.ofMillis(100));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);

            clock.forward(Duration.ofMillis(100));

            StepResult result = (partitionId.equals(succeededPartition) ? StepResult.success() : StepResult.retry());
            history.recordActivityResult(partitionId, result);

            clock.rewind(Duration.ofMillis(100));
        }

        // move back ahead of the partitions
        clock.forward(Duration.ofMillis(150));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assertions.assertEquals(retriedPartitions.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(retriedPartitions);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assertions.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assertions.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
    }

    @Test
    public void decide_schedulePartitionedSecondStep_OnePartitionSucceeded_OnePartitionScheduleActivityFailed() throws JsonProcessingException {
        List<String> partitionIds = new ArrayList<>();
        String succeededPartition = "p1";
        String failedPartition = "p2";
        partitionIds.add(succeededPartition);
        partitionIds.add(failedPartition);

        String additionalAttributeName = "AdditionalPartitionGeneratorData";
        Long additionalAttributeValue = 1234L;
        Map<String, Object> additionalAttributes = new HashMap<>();
        additionalAttributes.put(additionalAttributeName, additionalAttributeValue);

        TestPartitionedStep partitionedStep = (TestPartitionedStep)workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        partitionedStep.setPartitionIds(partitionIds);
        partitionedStep.setAdditionalAttributes(additionalAttributes);

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        history.scheduleStepAttempt(succeededPartition);
        history.recordScheduleAttemptFailed(failedPartition);

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(succeededPartition, StepResult.success());

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assertions.assertEquals(1, response.decisions().size());
        Decision decision = response.decisions().get(0);

        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
        Assertions.assertEquals(failedPartition, partitionId);

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
        expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
        expectedInput.put(additionalAttributeName, StepAttributes.encode(additionalAttributeValue));

        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(partitionedStep, 0, partitionId);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                expectedInput, activityId);
        attrs = attrs.toBuilder().control(partitionId).build();

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        clock.forward(Duration.ofMillis(100));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);

            clock.forward(Duration.ofMillis(100));

            StepResult result = (partitionId.equals(failedPartition) ? StepResult.failure() : StepResult.retry());
            history.recordActivityResult(partitionId, result);

            clock.rewind(Duration.ofMillis(100));
        }

        // move back ahead of the partitions
        clock.forward(Duration.ofMillis(150));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assertions.assertEquals(retriedPartitions.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(retriedPartitions);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.startTimerDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

            // allow jitter will make the retry time inconsistent, but as long as it's within 2 seconds it's fine for this test
            Assertions.assertEquals(10, Long.parseLong(decision.startTimerDecisionAttributes().startToFireTimeout()), 2);
            Assertions.assertEquals(activityId, decision.startTimerDecisionAttributes().timerId());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);

            clock.forward(Duration.ofSeconds(1));
            history.recordActivityResult(partitionId, StepResult.retry());

            clock.forward(Duration.ofSeconds(1));
            history.startRetryTimer(partitionId, Duration.ofSeconds(10));

            clock.forward(Duration.ofSeconds(10));
            history.closeRetryTimer(partitionId, false);

            clock.rewind(Duration.ofSeconds(12));
        }

        clock.forward(Duration.ofSeconds(13));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflowWithPartitionedStep.getGraph().getNodes().get(TestPartitionedStep.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepTwo);

        mockery.verify();

        Assertions.assertEquals(partitionIds.size(), response.decisions().size());
        Set<String> remainingPartitionsToSchedule = new HashSet<>(partitionIds);
        for (Decision decision : response.decisions()) {
            String partitionId = decision.scheduleActivityTaskDecisionAttributes().control();
            Assertions.assertNotNull(partitionId);
            Assertions.assertTrue(remainingPartitionsToSchedule.contains(partitionId));
            remainingPartitionsToSchedule.remove(partitionId);

            Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

            Map<String, String> expectedInput = new TreeMap<>(input);
            expectedInput.putAll(output);
            expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            expectedInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(1));

            expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
            expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

            String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionId);
            ScheduleActivityTaskDecisionAttributes attrs
                    = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                    expectedInput, activityId);
            attrs = attrs.toBuilder().control(partitionId).build();

            Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
        }

        Assertions.assertTrue(remainingPartitionsToSchedule.isEmpty());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        Duration stepTwoDuration = Duration.ofMillis(100);

        for (String partitionId : partitionIds) {
            history.scheduleStepAttempt(partitionId);
            clock.forward(stepTwoDuration);

            history.recordActivityResult(partitionId, StepResult.success());
            clock.rewind(stepTwoDuration);
        }

        clock.forward(stepTwoDuration.multipliedBy(2));

        Instant stepThreeInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> stepMetrics, clock);
        WorkflowStep stepThree = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class).getStep();
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, stepThree);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        String partitionedStepName = TaskNaming.activityName(workflowWithPartitionedStepName, partitionedStep);
        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepThreeInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepThree, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, stepThree,
                expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_scheduleSecondStepWhenFirstStepSucceeds_FirstStepHadMultipleAttempts() throws JsonProcessingException {
        Assertions.assertTrue(workflow.getGraph().getNodes().size() > 1);
        String activityName = TaskNaming.activityName(workflowName, workflow.getGraph().getFirstStep());

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        for (int i = 0; i < 4; i++) {
            history.scheduleStepAttempt();

            clock.forward(Duration.ofMillis(100));
            history.recordActivityResult(StepResult.retry());

            clock.forward(Duration.ofMillis(100));
            history.startRetryTimer(Duration.ofSeconds(10));

            clock.forward(Duration.ofSeconds(10));
            history.closeRetryTimer(false);

            clock.forward(Duration.ofMillis(100));
        }

        history.scheduleStepAttempt();

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        Duration stepOneTotalDuration = Duration.between(stepOneInitialAttemptTime, stepOneEndTime);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, stepTwo);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);

        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(stepTwo, 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, stepTwo,
                                                                            expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        Assertions.assertEquals(stepOneTotalDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(5, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_scheduleStepWithCustomHeartbeatTimeout() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithCustomHeartbeatTimeout, clock, input);
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithCustomHeartbeatTimeout, null, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> stepMetrics, clock);
        validateExecutionContext(response.executionContext(), workflowWithCustomHeartbeatTimeout, workflowWithCustomHeartbeatTimeout.getGraph().getFirstStep());

        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assertions.assertEquals(
                Long.toString(workflowWithCustomHeartbeatTimeout.getGraph().getFirstStep().activityTaskHeartbeatTimeout().getSeconds()),
                decision.scheduleActivityTaskDecisionAttributes().heartbeatTimeout());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleFirstStepWhenFirstStepNeedsRetry_StartTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> stepMetrics, clock);

        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(currentStep, 1, null);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assertions.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assertions.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_scheduleFirstStep_ScheduleActivityFailedEventForPreviousScheduleAttempt() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.recordScheduleAttemptFailed();

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, null, state.getWorkflowId(), state,
                FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> stepMetrics, clock);

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepOneInitialAttemptTime));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                input, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleFirstStepWhenFirstStepNeedsRetry_TimerFired() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> stepMetrics, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepOneInitialAttemptTime));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(currentStep, 1, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_rescheduleStepWhenRetryNowSignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordRetryNowSignal();

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assertions.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(activityId).build(), decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(1));

        mockery.verify();
    }

    @Test
    public void decide_IgnoreRetryNowSignalIfNoPreviousTimer_SchedulesNormalRetryTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.recordRetryNowSignal();

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assertions.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assertions.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_IgnoreRetryNowSignalIfTimerAlreadyClosed_SchedulesNextActivityAttempt() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        clock.forward(Duration.ofMillis(100));
        history.recordRetryNowSignal();

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepOneInitialAttemptTime));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, currentStep, input,
                                                                            TaskNaming.createActivityId(currentStep, 1, null));

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_sendScheduleDelayedRetrySignalWhenDelayRetrySignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assertions.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(activityId).build(), decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should to send the workflow a ScheduleDelayedRetry signal.
        Assertions.assertEquals(DecisionType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION, decisions.get(1).decisionType());
        DelayRetrySignalData signalData = new DelayRetrySignalData();
        signalData.setActivityId(activityId);
        signalData.setDelayInSeconds(142);
        Assertions.assertEquals(DecisionTaskPoller.buildScheduleRetrySignalAttrs(state, signalData),
                            decisions.get(1).signalExternalWorkflowExecutionDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_IgnoreDelayRetrySignalIfNoOpenTimer_SchedulesNormalRetryTimer() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(workflow.getGraph().getFirstStep(), 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, delayInSeconds, null);

        Assertions.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assertions.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_IgnoreDelayRetrySignalIfTimerAlreadyClosed_SchedulesNextActivityAttempt() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        clock.forward(Duration.ofMillis(100));

        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepOneInitialAttemptTime));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));
        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, currentStep, input,
                                                                            TaskNaming.createActivityId(currentStep, 1, null));

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

        mockery.verify();
    }

    @Test
    public void decide_createsDelayedRetryTimerWhenScheduleDelayedRetrySignalReceived() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        // these two happen at the same time since they're in the same decision
        clock.forward(Duration.ofMillis(100));
        history.closeRetryTimer(true);
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        String activityId = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityId, 142, null);

        Assertions.assertEquals(attrs.timerId(), decision.startTimerDecisionAttributes().timerId());
        Assertions.assertNotNull(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertTrue(0 < Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout()));

        mockery.verify();
    }

    @Test
    public void decide_ignoresScheduleDelayedRetrySignalIfTimerAlreadyOpen() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofMillis(100));
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, currentStep);
        List<Decision> decisions = response.decisions();
        Assertions.assertTrue(decisions.isEmpty(), decisions.toString());

        mockery.verify();
    }

    @Test
    public void decide_scheduleNextAttemptAfterDelayedRetryTimerFired() throws JsonProcessingException {
        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        // these two events are in the same decision and happen at the same time
        clock.forward(Duration.ofMillis(100));
        history.closeRetryTimer(true);
        history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(142));

        clock.forward(Duration.ofSeconds(142));
        history.closeRetryTimer(false);

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);

        validateExecutionContext(response.executionContext(), workflow, currentStep);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        input.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        input.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));
        input.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepOneInitialAttemptTime));
        input.put(StepAttributes.RETRY_ATTEMPT, Integer.toString(1));

        String activityIdSecondAttempt = TaskNaming.createActivityId(workflow.getGraph().getFirstStep(), 1, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflow, workflow.getGraph().getFirstStep(),
                                                                            input, activityIdSecondAttempt);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());

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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        // both partitions are initially scheduled at the same time
        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(succeededPartition);
        history.scheduleStepAttempt(retryPendingPartition);

        // for convenience we'll say they finished at the same time
        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(succeededPartition, StepResult.success());
        history.recordActivityResult(retryPendingPartition, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerEvent = history.startRetryTimer(retryPendingPartition, Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordRetryNowSignal(retryPendingPartition);

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assertions.assertEquals(2, decisions.size());

        // first decision should be to cancel the current timer
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(0).decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timerEvent.timerStartedEventAttributes().timerId()).build(),
                            decisions.get(0).cancelTimerDecisionAttributes());

        // second decision should always force a new decision
        Assertions.assertEquals(DecisionTaskPoller.decisionToForceNewDecision(), response.decisions().get(1));
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        // both partitions initially scheduled at the same time
        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(succeededPartition);
        history.scheduleStepAttempt(partitionToForce);

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(succeededPartition, StepResult.success());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        Instant stepTwoEndTime = clock.forward(Duration.ofSeconds(3));
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        Duration stepTwoDuration = Duration.between(stepTwoInitialAttemptTime, stepTwoEndTime);

        Instant stepThreeInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowGraphNode nextStep = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, nextStep.getStep());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assertions.assertEquals(1, decisions.size());
        Decision decision = decisions.get(0);

        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepThreeInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(nextStep.getStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, nextStep.getStep(),
                                                                            expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
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
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        // both partitions initially scheduled at the same time
        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(retryingPartition);
        history.scheduleStepAttempt(partitionToForce);

        // for convenience we'll say they finished at the same time
        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(retryingPartition, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(retryingPartition, Duration.ofSeconds(10));
        history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));


        clock.forward(Duration.ofSeconds(2));
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);
        clock.forward(Duration.ofSeconds(1));
        history.closeRetryTimer(partitionToForce, true);

        clock.forward(Duration.ofSeconds(7));
        history.closeRetryTimer(retryingPartition, false);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(retryingPartition);

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(retryingPartition, StepResult.success());

        Duration stepTwoDuration = Duration.between(stepTwoInitialAttemptTime, stepTwoEndTime);

        Instant stepThreeInitialAttemptTime = clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        WorkflowGraphNode nextStep = workflowWithPartitionedStep.getGraph().getNodes().get(TestStepHasOptionalInputAttribute.class);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, nextStep.getStep());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        mockery.verify();

        Assertions.assertEquals(1, response.decisions().size());

        Decision decision = response.decisions().get(0);

        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decision.decisionType());

        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(partitionedStepName)));
        Assertions.assertEquals(2, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(partitionedStepName)).longValue());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepThreeInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(nextStep.getStep(), 0, null);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, nextStep.getStep(),
                expectedInput, activityId);

        Assertions.assertEquals(attrs, decision.scheduleActivityTaskDecisionAttributes());
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        // both partitions initially scheduled at the same time
        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(retryingPartition);
        history.scheduleStepAttempt(partitionToForce);

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(retryingPartition, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(retryingPartition, Duration.ofSeconds(10));
        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        clock.forward(Duration.ofMillis(100));
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        clock.forward(Duration.ofMillis(100));
        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        Decision decision = response.decisions().get(0);
        mockery.verify();

        // only decision should be to cancel the current timer
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
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

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(partitionToReschedule);
        history.scheduleStepAttempt(partitionToForce);

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(partitionToReschedule, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(2));
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assertions.assertEquals(2, decisions.size());

        // first decision should be to schedule the retry timer for partition p1
        Assertions.assertEquals(DecisionType.START_TIMER, decisions.get(0).decisionType());

        String activityP1Id = TaskNaming.createActivityId(partitionedStep, 1, partitionToReschedule);
        long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(partitionedStep, 1,
                                                                        FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
        StartTimerDecisionAttributes attrs = DecisionTaskPoller.buildStartTimerDecisionAttrs(activityP1Id, delayInSeconds, null);

        Assertions.assertEquals(attrs.timerId(), decisions.get(0).startTimerDecisionAttributes().timerId());
        Assertions.assertNotNull(decisions.get(0).startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertTrue(0 < Integer.parseInt(decisions.get(0).startTimerDecisionAttributes().startToFireTimeout()));

        // second decision should be to cancel the p2 timer
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(1).decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
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

        Map<String, String> input = new TreeMap<>();
        input.put("SomeInput", "Value");

        Map<String, String> output = new TreeMap<>();
        output.put("ExtraOutput", "AnotherValue");

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithPartitionedStep, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(new HashSet<>(partitionIds));

        history.recordPartitionMetadataMarkers(clock.instant(), TaskNaming.stepName(partitionedStep), partitionIdGeneratorResult);

        Instant stepTwoInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt(partitionToReschedule);
        history.scheduleStepAttempt(partitionToForce);

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(partitionToReschedule, StepResult.retry());
        history.recordActivityResult(partitionToForce, StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(partitionToReschedule, Duration.ofSeconds(10));
        HistoryEvent timer2Event = history.startRetryTimer(partitionToForce, Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer(partitionToReschedule, false);
        history.recordForceResultSignal(partitionToForce, StepResult.SUCCEED_RESULT_CODE);

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        String workflowId = state.getWorkflowId();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithPartitionedStep, partitionedStep, workflowId, state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflowWithPartitionedStep, partitionedStep);
        List<Decision> decisions = response.decisions();
        mockery.verify();

        Assertions.assertEquals(2, decisions.size());

        // first decision should be to reschedule partition p1
        Assertions.assertEquals(DecisionType.SCHEDULE_ACTIVITY_TASK, decisions.get(0).decisionType());

        Map<String, String> expectedInput = new TreeMap<>(input);
        expectedInput.putAll(output);
        expectedInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionToReschedule));
        expectedInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
        expectedInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(1));
        expectedInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(stepTwoInitialAttemptTime));
        expectedInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
        expectedInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
        expectedInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(state.getWorkflowStartDate()));

        String activityId = TaskNaming.createActivityId(partitionedStep, 1, partitionToReschedule);
        ScheduleActivityTaskDecisionAttributes attrs
                = DecisionTaskPoller.buildScheduleActivityTaskDecisionAttrs(workflowWithPartitionedStep, partitionedStep,
                                                                            expectedInput, activityId);
        attrs = attrs.toBuilder().control(partitionToReschedule).build();

        Assertions.assertEquals(attrs, decisions.get(0).scheduleActivityTaskDecisionAttributes());

        // second decision should be to cancel the p2 timer
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decisions.get(1).decisionType());
        Assertions.assertEquals(CancelTimerDecisionAttributes.builder().timerId(timer2Event.timerStartedEventAttributes().timerId()).build(),
                            decisions.get(1).cancelTimerDecisionAttributes());
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_PreviousStepAlreadySucceeded() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.success());

        Instant workflowCancelRequestDate = clock.forward(Duration.ofMillis(100));
        history.recordCancelWorkflowExecutionRequest();

        // we move the clock a bit to be sure that the workflow close time is the cancel requested time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), workflowCancelRequestDate);

        mockery.replay();

        Assertions.assertEquals(TaskNaming.activityName(workflow, TestStepOne.class), state.getCurrentActivityName());
        WorkflowStep currentStep = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assertions.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)));
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)));
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelsInProgressActivity() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant workflowCancelRequestDate = clock.forward(Duration.ofMillis(100));
        history.recordCancelWorkflowExecutionRequest();

        // we move the clock a bit to be sure that the workflow close time is the cancel requested time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration stepOneDuration = Duration.between(stepOneInitialAttemptTime, workflowCancelRequestDate);
        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), workflowCancelRequestDate);

        mockery.replay();

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepOne, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, null);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.REQUEST_CANCEL_ACTIVITY_TASK, decision.decisionType());

        RequestCancelActivityTaskDecisionAttributes activityAttrs = RequestCancelActivityTaskDecisionAttributes.builder()
                .activityId(TaskNaming.createActivityId(stepOne, 0, null)).build();
        Assertions.assertEquals(activityAttrs, decision.requestCancelActivityTaskDecisionAttributes());

        decision = response.decisions().get(1);
        Assertions.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assertions.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)));
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)));
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelsOpenRetryTimer() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timer1Event = history.startRetryTimer(Duration.ofSeconds(10));

        Instant workflowCancelRequestDate = clock.forward(Duration.ofSeconds(2));
        history.recordCancelWorkflowExecutionRequest();

        // we move the clock a bit to be sure that the workflow close time is the cancel requested time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration stepOneDuration = Duration.between(stepOneInitialAttemptTime, workflowCancelRequestDate);
        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), workflowCancelRequestDate);

        mockery.replay();

        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepOne, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, null);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());

        CancelTimerDecisionAttributes timerAttrs = CancelTimerDecisionAttributes.builder()
                .timerId(timer1Event.timerStartedEventAttributes().timerId())
                .build();
        Assertions.assertEquals(timerAttrs, decision.cancelTimerDecisionAttributes());

        decision = response.decisions().get(1);
        Assertions.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assertions.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String stepOneActivityName = TaskNaming.activityName(workflowName, stepOne);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepOneActivityName)));
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)));
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_cancelsWorkflowWhenCancelRequestReceived_CancelIsHigherPriorityThanSignal() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);

        Instant stepOneInitialAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timer1Event = history.startRetryTimer(Duration.ofSeconds(10));

        Instant workflowCancelRequestDate = clock.forward(Duration.ofSeconds(2));
        history.recordCancelWorkflowExecutionRequest();

        clock.forward(Duration.ofMillis(100));
        history.recordRetryNowSignal();

        // we move the clock a bit to be sure that the workflow close time is the cancel requested time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration stepOneDuration = Duration.between(stepOneInitialAttemptTime, workflowCancelRequestDate);
        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), workflowCancelRequestDate);

        mockery.replay();

        WorkflowStep currentStep = workflow.getGraph().getFirstStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, null);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(2, response.decisions().size());
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.CANCEL_TIMER, decision.decisionType());

        CancelTimerDecisionAttributes timerAttrs = CancelTimerDecisionAttributes.builder()
                .timerId(timer1Event.timerStartedEventAttributes().timerId())
                .build();
        Assertions.assertEquals(timerAttrs, decision.cancelTimerDecisionAttributes());

        decision = response.decisions().get(1);
        Assertions.assertEquals(DecisionType.CANCEL_WORKFLOW_EXECUTION, decision.decisionType());

        CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
        Assertions.assertEquals(attrs, decision.cancelWorkflowExecutionDecisionAttributes());

        String activityName = TaskNaming.activityName(workflowName, currentStep);
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)));
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_completesWorkflowWhenLastStepSucceeds() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepTwoDuration = Duration.ofMillis(100);
        Instant stepTwoEndTime = clock.forward(stepTwoDuration);
        history.recordActivityResult(StepResult.success());

        // we move the clock a bit to be sure that the workflow close time is the step end time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), stepTwoEndTime);

        mockery.replay();

        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflow, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.COMPLETE_WORKFLOW_EXECUTION, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        CompleteWorkflowExecutionDecisionAttributes attrs = CompleteWorkflowExecutionDecisionAttributes.builder().build();
        Assertions.assertEquals(attrs, decision.completeWorkflowExecutionDecisionAttributes());

        String stepTwoActivityName = TaskNaming.activityName(workflowName, stepTwo);
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowName)));
        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_failsWorkflowWhenStepWithCloseOnFailureTransitionFails() throws JsonProcessingException {
        Workflow workflowWithFailureTransition = new TestWorkflowWithFailureTransition();
        String workflowWithFailureTransitionName = TaskNaming.workflowName(workflowWithFailureTransition);

        WorkflowStep currentStep = workflowWithFailureTransition.getGraph().getFirstStep();
        String activityName = TaskNaming.activityName(workflowWithFailureTransitionName, currentStep);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflowWithFailureTransition, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepOneDuration = Duration.ofMillis(100);
        Instant stepTwoEndTime = clock.forward(stepOneDuration);
        history.recordActivityResult(StepResult.failure());

        // we move the clock a bit to be sure that the workflow close time is the step end time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), stepTwoEndTime);

        mockery.replay();

        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(workflowWithFailureTransition, currentStep, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), workflowWithFailureTransition, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.FAIL_WORKFLOW_EXECUTION, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertNotNull(decision.failWorkflowExecutionDecisionAttributes().reason());
        Assertions.assertTrue(decision.failWorkflowExecutionDecisionAttributes().reason().startsWith(activityName + " failed after"));
        Assertions.assertNotNull("FailWorkflowExecutionDecision detail should not be null!", decision.failWorkflowExecutionDecisionAttributes().details());

        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(workflowWithFailureTransitionName)));
        Assertions.assertEquals(stepOneDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(activityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(activityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        mockery.verify();
    }

    @Test
    public void decide_periodicWorkflowSchedulesDelayExitTimerWhenLastStepSucceeds() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepTwoDuration = Duration.ofMillis(100);
        Instant stepTwoEndTime = clock.forward(stepTwoDuration);
        history.recordActivityResult(StepResult.success());

        // we move the clock a bit to be sure that the workflow close time is the step end time, not the current time
        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), stepTwoEndTime);

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, decision.startTimerDecisionAttributes().timerId());

        mockery.verify();

        // we want to see a workflow execution time metric emitted when the periodic workflow ends, before the delayed exit
        String stepTwoActivityName = TaskNaming.activityName(periodicWorkflowName, stepTwo);
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)));
        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        Periodic p = periodicWorkflow.getClass().getAnnotation(Periodic.class);

        // the timer should be set to end such that the workflow will be run once per period;
        // so the actual delay scheduled should be within a few seconds of the remaining time.
        long timeSinceWorkflowStart = clock.millis() - state.getWorkflowStartDate().toEpochMilli();
        long expectedDelay = p.intervalUnits().toSeconds(p.runInterval()) - (timeSinceWorkflowStart/1000);
        long allowedDelta = 2;
        // start-to-fire timeout is in seconds
        long actualDelay = Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertEquals(expectedDelay, actualDelay, allowedDelta);
    }

    /**
     * This test exists because Flux used to use joda's Duration to calculate durations, but joda's Duration
     * throws an exception if your duration is more than 30 days. java 8's Duration behaves sanely.
     */
    @Test
    public void decide_periodicWorkflowSchedulesDelayExitTimerWhenLastStepSucceeds_StepTookMoreThan30Days() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Duration stepTwoDuration = Duration.ofDays(45);
        Instant stepTwoEndTime = clock.forward(stepTwoDuration);
        history.recordActivityResult(StepResult.success());

        WorkflowState state = history.buildCurrentState();

        Duration workflowDuration = Duration.between(state.getWorkflowStartDate(), stepTwoEndTime);

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.START_TIMER, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Assertions.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, decision.startTimerDecisionAttributes().timerId());

        mockery.verify();

        // we want to see a workflow execution time metric emitted when the periodic workflow ends, before the delayed exit
        String stepTwoActivityName = TaskNaming.activityName(periodicWorkflowName, stepTwo);
        Assertions.assertEquals(workflowDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)));
        Assertions.assertEquals(stepTwoDuration, deciderMetrics.getDurations().get(DecisionTaskPoller.formatStepCompletionTimeMetricName(stepTwoActivityName)));
        Assertions.assertEquals(1, deciderMetrics.getCounts().get(DecisionTaskPoller.formatStepAttemptCountForCompletionMetricName(stepTwoActivityName)).longValue());
        Assertions.assertEquals(state.getWorkflowId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(state.getWorkflowRunId(), deciderMetrics.getProperties().get(DecisionTaskPoller.WORKFLOW_RUN_ID_METRIC_NAME));

        // the timer should be set to 1 second since the workflow ran for 45 days and our minimum delay time is 1 second.
        long actualDelay = Integer.parseInt(decision.startTimerDecisionAttributes().startToFireTimeout());
        Assertions.assertEquals(1.0, actualDelay, 0.0);
    }

    @Test
    public void decide_periodicWorkflowContinuesAsNewWorkflowWhenDelayExitTimerFires() throws JsonProcessingException {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicWorkflow, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerEvent = history.startDelayExitTimer();

        long timerSeconds = Long.parseLong(timerEvent.timerStartedEventAttributes().startToFireTimeout());
        clock.forward(Duration.ofSeconds(timerSeconds));
        history.closeDelayExitTimer(false);

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicWorkflow, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), periodicWorkflow, null);
        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION, decision.decisionType());

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        ContinueAsNewWorkflowExecutionDecisionAttributes attrs = ContinueAsNewWorkflowExecutionDecisionAttributes.builder()
                .childPolicy(ChildPolicy.TERMINATE)
                .input(StepAttributes.encode(Collections.emptyMap()))
                .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                .executionStartToCloseTimeout(Long.toString(periodicWorkflow.maxStartToCloseDuration().getSeconds()))
                .taskList(TaskList.builder().name(periodicWorkflow.taskList()).build())
                .build();
        Assertions.assertEquals(attrs, decision.continueAsNewWorkflowExecutionDecisionAttributes());

        // we do *not* want to see a workflow execution time metric emitted after the delayed exit ends
        Assertions.assertFalse(deciderMetrics.getDurations().containsKey(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicWorkflowName)));

        mockery.verify();
    }

    @Test
    public void decide_periodicWorkflowContinuesAsNewWorkflowWhenDelayExitTimerFires_CustomTaskList() throws JsonProcessingException {
        Workflow periodicCustomTaskList = new TestPeriodicWorkflowCustomTaskList();
        String periodicCustomTaskListWorkflowName = TaskNaming.workflowName(periodicCustomTaskList);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(periodicCustomTaskList, clock);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerEvent = history.startDelayExitTimer();

        long timerSeconds = Long.parseLong(timerEvent.timerStartedEventAttributes().startToFireTimeout());
        clock.forward(Duration.ofSeconds(timerSeconds));
        history.closeDelayExitTimer(false);

        clock.forward(Duration.ofMillis(100));

        WorkflowState state = history.buildCurrentState();

        mockery.replay();

        WorkflowStep stepTwo = periodicCustomTaskList.getGraph().getNodes().get(TestStepTwo.class).getStep();
        RespondDecisionTaskCompletedRequest response = DecisionTaskPoller.decide(periodicCustomTaskList, stepTwo, state.getWorkflowId(), state,
                                                                                 FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE,
                                                                                 deciderMetrics,  (o, c) -> { throw new RuntimeException("shouldn't request stepMetrics"); }, clock);
        validateExecutionContext(response.executionContext(), periodicCustomTaskList, null);

        // deciderMetrics is normally closed by the poll method.
        // we need to close it here so we can query the metrics.
        deciderMetrics.close();

        Decision decision = response.decisions().get(0);
        Assertions.assertEquals(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION, decision.decisionType());

        ContinueAsNewWorkflowExecutionDecisionAttributes attrs = ContinueAsNewWorkflowExecutionDecisionAttributes.builder()
                .childPolicy(ChildPolicy.TERMINATE)
                .input(StepAttributes.encode(Collections.emptyMap()))
                .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                .executionStartToCloseTimeout(Long.toString(periodicCustomTaskList.maxStartToCloseDuration().getSeconds()))
                .taskList(TaskList.builder().name(periodicCustomTaskList.taskList()).build())
                .build();
        Assertions.assertEquals(attrs, decision.continueAsNewWorkflowExecutionDecisionAttributes());

        // we do *not* want to see a workflow execution time metric emitted after the delayed exit ends
        Assertions.assertFalse(deciderMetrics.getDurations().containsKey(DecisionTaskPoller.formatWorkflowCompletionTimeMetricName(periodicCustomTaskListWorkflowName)));

        mockery.verify();
    }

    @Test
    public void findNextStep_FindsFirstStepWhenNoCurrentStep() {
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, null, null);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(workflow.getGraph().getFirstStep(), selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStepWhenCurrentIsFirstStep() {
        WorkflowStep stepOne = workflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(stepTwo, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_BranchingWorkflow_RespectsDefaultResultCode() {
        Workflow branchingWorkflow = new TestBranchingWorkflow();

        WorkflowStep stepOne = branchingWorkflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = branchingWorkflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(branchingWorkflow, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(stepTwo, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_BranchingWorkflow_RespectsCustomResultCode() {
        Workflow branchingWorkflow = new TestBranchingWorkflow();

        WorkflowStep stepOne = branchingWorkflow.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep branchStep = branchingWorkflow.getGraph().getNodes().get(TestBranchStep.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(branchingWorkflow, stepOne, TestBranchingWorkflow.CUSTOM_RESULT);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(branchStep, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_NoNextStepWhenCurrentStepIsLast() {
        WorkflowStep secondStep = workflow.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(workflow, secondStep, StepResult.SUCCEED_RESULT_CODE);
        Assertions.assertTrue(selection.workflowShouldClose());
        Assertions.assertNull(selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeSucceed() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, StepResult.SUCCEED_RESULT_CODE);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(stepTwo, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeFail() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, StepResult.FAIL_RESULT_CODE);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(stepTwo, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_FindsSecondStep_AlwaysTransition_ResultCodeCustom() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepOne = always.getGraph().getNodes().get(TestStepOne.class).getStep();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepOne, "some-random-custom-code");
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertEquals(stepTwo, selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeSucceed() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, StepResult.SUCCEED_RESULT_CODE);
        Assertions.assertTrue(selection.workflowShouldClose());
        Assertions.assertNull(selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeFail() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, StepResult.FAIL_RESULT_CODE);
        Assertions.assertTrue(selection.workflowShouldClose());
        Assertions.assertNull(selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_CloseWorkflowAfterSecondStep_AlwaysTransition_ResultCodeCustom() {
        TestWorkflowWithAlwaysTransition always = new TestWorkflowWithAlwaysTransition();
        WorkflowStep stepTwo = always.getGraph().getNodes().get(TestStepTwo.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(always, stepTwo, "some-random-custom-code");
        Assertions.assertTrue(selection.workflowShouldClose());
        Assertions.assertNull(selection.getNextStep());
        Assertions.assertFalse(selection.isNextStepUnknown());
    }

    @Test
    public void findNextStep_UnknownResultCode() {
        TestWorkflowDoesntHandleCustomResultCode wf = new TestWorkflowDoesntHandleCustomResultCode();
        WorkflowStep stepOne = wf.getGraph().getNodes().get(TestStepReturnsCustomResultCode.class).getStep();
        NextStepSelection selection = DecisionTaskPoller.findNextStep(wf, stepOne, TestStepReturnsCustomResultCode.RESULT_CODE);
        Assertions.assertFalse(selection.workflowShouldClose());
        Assertions.assertNull(selection.getNextStep());
        Assertions.assertTrue(selection.isNextStepUnknown());
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
        Assertions.assertEquals(300, RetryUtils.calculateRetryBackoffInSeconds(step, 1, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomMaxRetryDelay() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 10, maxRetryDelaySeconds = 20, jitterPercent = 0)
            public void apply() {}
        };

        // since jitterPercent is 0 and the max retry delay is 20, the 10000th retry should use 20 seconds
        Assertions.assertEquals(20, RetryUtils.calculateRetryBackoffInSeconds(step, 10000, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomBackoffBase() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 10, retriesBeforeBackoff = 6, jitterPercent = 0, exponentialBackoffBase = 1.25)
            public void apply() {}
        };

        // since jitterPercent is 0 the first 6 retries should use 10 seconds, then attempt 7 should use 12 seconds (10 * (1.25 ^ 1)),
        // and attempt 8 should use 15 seconds (10 * (1.25 ^ 2))
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 1, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 2, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 3, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 4, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 5, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(10, RetryUtils.calculateRetryBackoffInSeconds(step, 6, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(12, RetryUtils.calculateRetryBackoffInSeconds(step, 7, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        Assertions.assertEquals(15, RetryUtils.calculateRetryBackoffInSeconds(step, 8, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
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
            Assertions.assertTrue(calculatedTime >= 90 && calculatedTime <= 110);
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
            Assertions.assertTrue(calculatedTime >= 10 && calculatedTime <= 190);
            if (calculatedTime < 90 || calculatedTime > 110) {
                foundLargerJitter = true;
            }
        }
        Assertions.assertTrue(foundLargerJitter);
    }

    @Test
    public void testCalculateRetryBackoff_RespectsCustomRetriesBeforeBackoff() {
        WorkflowStep step = new WorkflowStep() {
            @StepApply(initialRetryDelaySeconds = 300, maxRetryDelaySeconds = 3000, jitterPercent = 0, retriesBeforeBackoff = 27)
            public void apply() {}
        };

        // since jitterPercent is 0 and the initial retry delay is 300, the first 27 retries should use 300 seconds
        for (int attempt = 1; attempt <= 27; attempt++) {
            Assertions.assertEquals(300, RetryUtils.calculateRetryBackoffInSeconds(step, attempt, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE));
        }
        // the 28th retry should delay longer than 300 seconds
        Assertions.assertTrue(RetryUtils.calculateRetryBackoffInSeconds(step, 28, FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE) > 300);
    }

    @Test
    public void testDecisionToForceNewDecision() {
        StartTimerDecisionAttributes expectedAttrs
                = DecisionTaskPoller.buildStartTimerDecisionAttrs(DecisionTaskPoller.FORCE_NEW_DECISION_TIMER_ID, 0, null);
        Decision expected = Decision.builder().decisionType(DecisionType.START_TIMER)
                .startTimerDecisionAttributes(expectedAttrs).build();

        Assertions.assertEquals(expected, DecisionTaskPoller.decisionToForceNewDecision());
    }

    private void validateExecutionContext(String executionContext, Workflow workflow, WorkflowStep expectedNextStep) {
        if (expectedNextStep == null) {
            if (workflow.getClass().isAnnotationPresent(Periodic.class)) {
                Assertions.assertNotNull(executionContext);
                Map<String, String> decodedExecutionContext = StepAttributes.decode(Map.class, executionContext);
                Assertions.assertNotNull(decodedExecutionContext);
                Assertions.assertEquals(1, decodedExecutionContext.size());
                Assertions.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, StepAttributes.decode(String.class, decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_NAME)));
            } else {
                Assertions.assertNull(executionContext);
            }
        } else {
            Assertions.assertNotNull(executionContext);
            Map<String, String> decodedExecutionContext = StepAttributes.decode(Map.class, executionContext);
            Assertions.assertNotNull(decodedExecutionContext);
            Assertions.assertEquals(2, decodedExecutionContext.size());
            Assertions.assertEquals(expectedNextStep.getClass().getSimpleName(), StepAttributes.decode(String.class, decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_NAME)));

            String encodedResultCodes = decodedExecutionContext.get(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_RESULT_CODES);
            Assertions.assertNotNull(encodedResultCodes);
            Map<String, String> decodedResultCodeMap = StepAttributes.decode(Map.class, encodedResultCodes);
            Assertions.assertNotNull(decodedResultCodeMap);
            Assertions.assertEquals(workflow.getGraph().getNodes().get(expectedNextStep.getClass()).getNextStepsByResultCode().size(), decodedResultCodeMap.size());
            for (Map.Entry<String, WorkflowGraphNode> resultCodes : workflow.getGraph().getNodes().get(expectedNextStep.getClass()).getNextStepsByResultCode().entrySet()) {
                Assertions.assertTrue(decodedResultCodeMap.containsKey(resultCodes.getKey()));
                if (resultCodes.getValue() == null) {
                    Assertions.assertEquals(DecisionTaskPoller.EXECUTION_CONTEXT_NEXT_STEP_RESULT_WORKFLOW_ENDS, decodedResultCodeMap.get(resultCodes.getKey()));
                } else {
                    Assertions.assertEquals(resultCodes.getValue().getStep().getClass().getSimpleName(), decodedResultCodeMap.get(resultCodes.getKey()));
                }
            }
        }
    }
}
