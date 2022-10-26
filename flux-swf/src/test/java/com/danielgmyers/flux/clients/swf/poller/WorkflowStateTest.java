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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.clients.swf.IdUtils;
import com.danielgmyers.flux.clients.swf.poller.signals.DelayRetrySignalData;
import com.danielgmyers.flux.clients.swf.poller.signals.ForceResultSignalData;
import com.danielgmyers.flux.clients.swf.poller.signals.SignalType;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestBranchingWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPartitionedStep;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepOne;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestStepTwo;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithFailureTransition;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithPartitionedStep;
import com.danielgmyers.flux.clients.swf.poller.timers.TimerData;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.StepResult;
import com.danielgmyers.flux.clients.swf.util.ManualClock;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.swf.model.ActivityTaskScheduledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class WorkflowStateTest {

    private ManualClock clock;

    @BeforeEach
    public void setup() {
        clock = new ManualClock();
    }

    @Test
    public void testBuild_ThrowsWithEmptyHistoryList() {
        try {
            Workflow workflow = new TestWorkflow();
            WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);
            PollForDecisionTaskResponse task = history.buildDecisionTask();
            WorkflowState.build(task.toBuilder().events(Collections.emptyList()).build());
            Assertions.fail();
        } catch(BadWorkflowStateException e) {
            // expected
        }
    }

    @Test
    public void testBuild_ThrowsIfNoWorkflowStartedEvent() {
        try {
            Workflow workflow = new TestWorkflow();
            WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock);
            history.scheduleStepAttempt();
            history.recordActivityResult(StepResult.success());

            PollForDecisionTaskResponse task = history.buildDecisionTask();
            List<HistoryEvent> eventsCopy = new ArrayList<>(task.events());
            Assertions.assertEquals(EventType.WORKFLOW_EXECUTION_STARTED, eventsCopy.get(eventsCopy.size()-1).eventType());
            eventsCopy.remove(eventsCopy.size()-1); // we'll remove the last event, which was the workflow started event

            WorkflowState.build(task.toBuilder().events(eventsCopy).build());
            Assertions.fail();
        } catch(BadWorkflowStateException e) {
            // expected
        }
    }

    @Test
    public void testBuild_WorkflowStarted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);
        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertNull(ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        // Shouldn't have any state for the first step of the workflow
        Assertions.assertEquals(Collections.emptyMap(),
                            ws.getLatestPartitionStates(TaskNaming.activityName(workflow, workflow.getGraph().getFirstStep())));
    }

    @Test
    public void testBuild_ActivityCompleted_Success() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Map<String, String> output = new HashMap<>();
        output.put("foo", "bar");
        output.put("baz", "zap");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success().withAttributes(output));

        clock.forward(Duration.ofMillis(100));
        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepOneEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertFalse(ws.getLatestPartitionStates(firstActivityName).isEmpty());
        Assertions.assertEquals(1, ws.getLatestPartitionStates(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, output, StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_ActivityCompleted_Failed() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Map<String, String> output = new HashMap<>();
        output.put("foo", "bar");
        output.put("baz", "zap");

        Workflow workflow = new TestWorkflowWithFailureTransition();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.failure().withAttributes(output));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepOneEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertFalse(ws.getLatestPartitionStates(firstActivityName).isEmpty());
        Assertions.assertEquals(1, ws.getLatestPartitionStates(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, output, StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);
    }

    @Test
    public void testBuild_ActivityCompleted_CustomResultCode() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Map<String, String> output = new HashMap<>();
        output.put("foo", "bar");
        output.put("baz", "zap");

        Workflow workflow = new TestBranchingWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        String completionMessage = "Custom result!";
        history.recordActivityResult(StepResult.complete(TestBranchingWorkflow.CUSTOM_RESULT, completionMessage).withAttributes(output));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepOneEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertEquals(TestBranchingWorkflow.CUSTOM_RESULT, ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertFalse(ws.getLatestPartitionStates(firstActivityName).isEmpty());
        Assertions.assertEquals(1, ws.getLatestPartitionStates(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, output, StepResult.ResultAction.COMPLETE, TestBranchingWorkflow.CUSTOM_RESULT, completionMessage);
    }

    @Test
    public void testBuild_ActivityCompleted_SignalOverridesCustomResultCode() throws JsonProcessingException {
        // This test simulates the decider not knowing what to do with a custom result code, so the user has sent
        // a ForceResult signal to override the result code.

        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Map<String, String> output = new HashMap<>();
        output.put("foo", "bar");
        output.put("baz", "zap");

        Workflow workflow = new TestBranchingWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        String completionMessage = "Custom result!";
        history.recordActivityResult(StepResult.complete(TestBranchingWorkflow.CUSTOM_RESULT, completionMessage).withAttributes(output));

        Instant signalTime = clock.forward(Duration.ofMillis(100));
        // we don't use history.recordForcedResultSignal since it helpfully signals the second step of the workflow,
        // because the first step completed successfully.
        // note we use retry attempt 1 even though there isn't an open timer, because WorkflowState only respects signals with
        // retry number = (last attempt + 1)
        ForceResultSignalData signal = new ForceResultSignalData();
        signal.setActivityId(TaskNaming.createActivityId(TestStepOne.class.getSimpleName(), 1, null));
        signal.setResultCode(StepResult.SUCCEED_RESULT_CODE);
        history.recordSignalEvent(signal);

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(signalTime, ws.getCurrentStepCompletionTime()); // the signal event's timestamp counts here
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertFalse(ws.getLatestPartitionStates(firstActivityName).isEmpty());
        Assertions.assertEquals(1, ws.getLatestPartitionStates(firstActivityName).size());

        // we evaluate the step result with the custom result code because the signal doesn't retroactively change the output
        // contents from the activity completion event.
        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, output, StepResult.ResultAction.COMPLETE, TestBranchingWorkflow.CUSTOM_RESULT, completionMessage);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionActivitiesRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_OnePartitionSucceededAndOneRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.success());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_OnePartitionFailedAndOneRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.failure());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_FirstPartitionSucceededAndSecondFailedToSchedule() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.recordScheduleAttemptFailed("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_FirstFailedToScheduleAndSecondSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.recordScheduleAttemptFailed("p1");
        history.scheduleStepAttempt("p2");

        history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsFailedToSchedule() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String firstStepName = TaskNaming.activityName(workflow, TestStepOne.class);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.recordScheduleAttemptFailed("p1");
        history.recordScheduleAttemptFailed("p2");

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstStepName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepOneEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompleted_BothSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.success());

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompleted_OneFailed_OneSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.failure());

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p1", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompletedAfterMultipleAttempts() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent p1Timer1 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer1 = history.startRetryTimer("p2", Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent p1Timer2 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer2 = history.startRetryTimer("p2", Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        Instant p1Attempt3Time = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");

        Instant p2Attempt3Time = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.success());

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertEquals(4, ws.getClosedTimers().size()); // two retries each for two partitions
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p1Timer1.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p2Timer1.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p1Timer2.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p2Timer2.timerStartedEventAttributes().timerId()));

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(2L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p1", 2, 3, p1Attempt3Time,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 3, p2Attempt3Time,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompletedAfterMultipleAttempts_OneFailed_OneSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent p1Timer1 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer1 = history.startRetryTimer("p2", Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent p1Timer2 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer2 = history.startRetryTimer("p2", Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        Instant p1Attempt3Time = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p1");

        Instant p2Attempt3Time = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt("p2");

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p1", StepResult.success());

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult("p2", StepResult.failure());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertEquals(4, ws.getClosedTimers().size()); // two retries each for two partitions
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p1Timer1.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p2Timer1.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p1Timer2.timerStartedEventAttributes().timerId()));
        Assertions.assertTrue(ws.getClosedTimers().containsKey(p2Timer2.timerStartedEventAttributes().timerId()));

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        // the effectiveResultCode should be _fail as one of the partitions has failed
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(2L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName));
        Assertions.assertFalse(ws.getLatestPartitionStates(partitionedStepName).isEmpty());
        Assertions.assertEquals(2, ws.getLatestPartitionStates(partitionedStepName).size());

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p1"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p1"));

        Assertions.assertTrue(ws.getLatestPartitionStates(partitionedStepName).containsKey("p2"));
        Assertions.assertNotNull(ws.getLatestPartitionStates(partitionedStepName).get("p2"));

        verifyStepResult(ws, partitionedStepName, "p1", 2, 3, p1Attempt3Time,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, 3, p2Attempt3Time,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);
    }

    @Test
    public void testBuild_ActivityTimedOut_Retry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityTimedOut();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_ActivityCanceled_Retry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityCanceled();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_MultipleStepAttemptsPresent_Success() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());
        int attempts = 1;

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        Set<String> closedTimers = new HashSet<>();

        clock.forward(Duration.ofSeconds(10));
        history.closeRetryTimer(false);
        closedTimers.add(timerStart.timerStartedEventAttributes().timerId());

        for (int i = 0; i < 1000; i++) {

            clock.forward(Duration.ofMillis(100));
            history.scheduleStepAttempt();

            clock.forward(Duration.ofMillis(100));
            history.recordActivityResult(StepResult.retry());

            clock.forward(Duration.ofMillis(100));
            timerStart = history.startRetryTimer(Duration.ofSeconds(10));

            clock.forward(Duration.ofSeconds(10));
            history.closeRetryTimer(false);
            closedTimers.add(timerStart.timerStartedEventAttributes().timerId());

            attempts++;
        }


        Instant stepOneLastAttemptTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();
        attempts++;

        Instant stepOneEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertEquals(closedTimers, ws.getClosedTimers().keySet());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepOneEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(1001L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, attempts, stepOneLastAttemptTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_UsesMostRecentStepWhenMultipleStepsInHistory_Success() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, 1, stepTwoStartTime,
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsOpenTimer() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertEquals(1, ws.getOpenTimers().size());
        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(new TimerData(timerStart), ws.getOpenTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_DetectsTimerFired() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        HistoryEvent timerFired = history.closeRetryTimer(false);

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());

        Assertions.assertEquals(1, ws.getClosedTimers().size());
        Assertions.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(timerFired.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

    }

    @Test
    public void testBuild_DetectsTimerCancelled() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        HistoryEvent timerCancelled = history.closeRetryTimer(true);

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());

        Assertions.assertEquals(1, ws.getClosedTimers().size());
        Assertions.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(timerCancelled.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_DetectsTimerThatWasCancelledAndRestarted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        history.closeRetryTimer(true);

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerRestart = history.startRetryTimer(Duration.ofSeconds(10));
        Assertions.assertEquals(timerStart.timerStartedEventAttributes().timerId(), timerRestart.timerStartedEventAttributes().timerId());

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerRestart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_DetectsTimerThatWasCancelledAndRestartedAndFiredNormally() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        history.closeRetryTimer(true);

        clock.forward(Duration.ofMillis(100));
        history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(10));
        HistoryEvent timerFired = history.closeRetryTimer(false);

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getOpenTimers().isEmpty());

        Assertions.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(timerFired.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_StoresRetryNowSignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());


        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        HistoryEvent signal = history.recordRetryNowSignal();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assertions.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_StoresRetryNowSignal_EvenIfSignalDataContainsUnknownFields() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        Map<String, String> rawSignalData = new HashMap<>();
        rawSignalData.put("activityId", timerStart.timerStartedEventAttributes().timerId());
        rawSignalData.put("someIrrelevantOperationalField", "Signal Sent By jtkirk");
        HistoryEvent signal = history.recordSignalEvent(SignalType.RETRY_NOW.getFriendlyName(), new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assertions.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_IgnoresRetryNowSignalWhenInputDataIsMissingFields() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        Map<String, String> rawSignalData = new HashMap<>();
        // intentionally not populating activity id
        history.recordSignalEvent(SignalType.RETRY_NOW.getFriendlyName(), new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_StoresDelayRetrySignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        HistoryEvent signal = history.recordDelayRetrySignal(Duration.ofSeconds(142));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.DELAY_RETRY, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assertions.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assertions.assertEquals(142, ((DelayRetrySignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getDelayInSeconds().intValue());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_StoresScheduleDelayedRetrySignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));

        clock.forward(Duration.ofMillis(100));
        HistoryEvent signal2 = history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.SCHEDULE_DELAYED_RETRY, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assertions.assertEquals(signal2.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assertions.assertEquals(142, ((DelayRetrySignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getDelayInSeconds().intValue());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_ForceResultSignalActuallyForcesResult() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        HistoryEvent signal = history.recordForceResultSignal(StepResult.FAIL_RESULT_CODE);

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.FORCE_RESULT, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assertions.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ((ForceResultSignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getResultCode());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(WorkflowState.FORCED_RESULT_MESSAGE, ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(signal.eventTimestamp(), ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_OnlyConsidersLatestSignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofSeconds(3));
        history.recordRetryNowSignal();

        clock.forward(Duration.ofMillis(100));
        HistoryEvent signal2 = history.recordRetryNowSignal(); // same signal, repeated

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assertions.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        // signal2 was second so it's the event that should be retained
        Assertions.assertEquals(signal2.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_IgnoresInvalidSignalType() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofMillis(100));
        Map<String, String> rawSignalData = new HashMap<>();
        rawSignalData.put("activityId", timerStart.timerStartedEventAttributes().timerId());
        history.recordSignalEvent("fake signal type", new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_IgnoresInvalidSignalData() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.retry());

        clock.forward(Duration.ofMillis(100));
        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        clock.forward(Duration.ofMillis(100));
        history.recordSignalEvent("fake signal type", "this is not valid json");

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());

        Assertions.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assertions.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepResultCode());
        Assertions.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepCompletionTime());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_DetectsWorkflowCancelRequest() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant cancelTime = clock.forward(Duration.ofMillis(100));
        history.recordCancelWorkflowExecutionRequest();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertTrue(ws.isWorkflowCancelRequested());
        Assertions.assertEquals(cancelTime, ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertNull(ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        // Shouldn't have any state for the first step of the workflow
        Assertions.assertEquals(Collections.emptyMap(),
                            ws.getLatestPartitionStates(TaskNaming.activityName(workflow, workflow.getGraph().getFirstStep())));
    }

    @Test
    public void testBuild_DetectsWorkflowCancelRequest_FirstStepInProgress() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant cancelTime = clock.forward(Duration.ofMillis(100));
        history.recordCancelWorkflowExecutionRequest();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertTrue(ws.isWorkflowCancelRequested());
        Assertions.assertEquals(cancelTime, ws.getWorkflowCancelRequestDate());
        Assertions.assertFalse(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(stepOneStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(0, (long)ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionCanceled() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant cancelTime = clock.forward(Duration.ofMillis(100));
        history.recordCancelWorkflowExecutionRequest();

        clock.forward(Duration.ofMillis(100));
        history.recordWorkflowCanceled();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertTrue(ws.isWorkflowCancelRequested());
        Assertions.assertEquals(cancelTime, ws.getWorkflowCancelRequestDate());
        Assertions.assertTrue(ws.isWorkflowExecutionClosed());

        Assertions.assertNull(ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        // Shouldn't have any state for the first step of the workflow
        Assertions.assertEquals(Collections.emptyMap(),
                            ws.getLatestPartitionStates(TaskNaming.activityName(workflow, workflow.getGraph().getFirstStep())));
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionCompleted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        history.recordWorkflowCompleted();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertTrue(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        Assertions.assertNotNull(ws.getLatestPartitionStates(secondActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(secondActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionFailed() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflowWithFailureTransition();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        Instant stepOneStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        Instant stepTwoStartTime = clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        Instant stepTwoEndTime = clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.failure());

        clock.forward(Duration.ofMillis(100));
        history.recordWorkflowFailed();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertTrue(ws.isWorkflowExecutionClosed());

        Assertions.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assertions.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assertions.assertEquals(stepTwoStartTime, ws.getCurrentStepFirstScheduledTime());
        Assertions.assertEquals(stepTwoEndTime, ws.getCurrentStepCompletionTime());
        Assertions.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(firstActivityName).get(null));

        Assertions.assertNotNull(ws.getLatestPartitionStates(secondActivityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(secondActivityName).get(null));

        verifyStepResult(ws, firstActivityName, null, 1, 1, stepOneStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, 1, stepTwoStartTime,
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionTerminated() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.recordWorkflowTerminated();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertTrue(ws.isWorkflowExecutionClosed());

        Assertions.assertNull(ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        // Shouldn't have any state for the first step of the workflow
        Assertions.assertEquals(Collections.emptyMap(),
                            ws.getLatestPartitionStates(TaskNaming.activityName(workflow, workflow.getGraph().getFirstStep())));
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionTimedOut() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant workflowStartTime = clock.instant();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, input);

        clock.forward(Duration.ofMillis(100));
        history.recordWorkflowTimedOut();

        WorkflowState ws = history.buildCurrentState();

        Assertions.assertEquals(workflowStartTime, ws.getWorkflowStartDate());
        Assertions.assertEquals(input, ws.getWorkflowInput());
        Assertions.assertTrue(ws.getOpenTimers().isEmpty());
        Assertions.assertTrue(ws.getClosedTimers().isEmpty());

        Assertions.assertFalse(ws.isWorkflowCancelRequested());
        Assertions.assertNull(ws.getWorkflowCancelRequestDate());
        Assertions.assertTrue(ws.isWorkflowExecutionClosed());

        Assertions.assertNull(ws.getCurrentActivityName());
        Assertions.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assertions.assertNull(ws.getCurrentStepMaxRetryCount());
        Assertions.assertNull(ws.getCurrentStepResultCode());

        // Shouldn't have any state for the first step of the workflow
        Assertions.assertEquals(Collections.emptyMap(),
                            ws.getLatestPartitionStates(TaskNaming.activityName(workflow, workflow.getGraph().getFirstStep())));
    }

    @Test
    public void testGetStepData_WorkflowStartedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ActivityScheduledEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        history.scheduleStepAttempt();
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ActivityCompletedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ActivityTimedOutEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        history.scheduleStepAttempt();
        history.recordActivityTimedOut();
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ActivityFailedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ActivityFailedViaException() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry(new RuntimeException()));
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        HistoryEvent event = task.events().get(0);
        Assertions.assertNotNull(WorkflowState.getStepData(event));
    }

    @Test
    public void testGetStepData_ThrowsForOtherEvent() {
        // I picked an event type here that we expect to never use
        HistoryEvent event = HistoryEvent.builder()
                .eventType(EventType.CHILD_WORKFLOW_EXECUTION_STARTED)
                .childWorkflowExecutionStartedEventAttributes(ChildWorkflowExecutionStartedEventAttributes.builder().build())
                .build();
        try {
            WorkflowState.getStepData(event);
            Assertions.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetStepName_WorkflowStartedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), clock);
        PollForDecisionTaskResponse task = history.buildDecisionTask();
        // the first event in the list is the most recent
        Assertions.assertNull(WorkflowState.getActivityName(task.events().get(0)));
    }

    @Test
    public void testGetStepName_ActivityScheduledEvent() {
        String stepName = TaskNaming.activityName(TestWorkflow.class, TestStepOne.class);
        ActivityType type = ActivityType.builder().name(stepName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();

        ActivityTaskScheduledEventAttributes attrs = ActivityTaskScheduledEventAttributes.builder().activityType(type).build();

        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_SCHEDULED)
                .activityTaskScheduledEventAttributes(attrs).build();

        Assertions.assertEquals(stepName, WorkflowState.getActivityName(event));
    }

    @Test
    public void testGetStepName_ThrowsForOtherEvent() {
        // I picked an event type here that we expect to never use
        HistoryEvent event = HistoryEvent.builder()
                .eventType(EventType.CHILD_WORKFLOW_EXECUTION_STARTED)
                .childWorkflowExecutionStartedEventAttributes(ChildWorkflowExecutionStartedEventAttributes.builder().build())
                .build();
        try {
            WorkflowState.getActivityName(event);
            Assertions.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetStepOutput_ActivityCompletedEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_COMPLETED).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assertions.assertEquals(StepResult.ResultAction.COMPLETE, result);
    }

    @Test
    public void testGetStepResultAction_ActivityTimedOutEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_TIMED_OUT).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assertions.assertEquals(StepResult.ResultAction.RETRY, result);
    }

    @Test
    public void testGetStepResultAction_ActivityFailedEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_FAILED).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assertions.assertEquals(StepResult.ResultAction.RETRY, result);
    }

    @Test
    public void testGetStepResultAction_ThrowsForOtherEvent() {
        try {
            HistoryEvent event = HistoryEvent.builder().eventType(EventType.CHILD_WORKFLOW_EXECUTION_COMPLETED).build();
            WorkflowState.getStepResultAction(event);
            Assertions.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetPartitionMetadata_NonPartitionedStep() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(partitionIds);

        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        history.recordPartitionMetadataMarkers(clock.instant(), stepName, partitionIdGeneratorResult);

        WorkflowState state = history.buildCurrentState();

        PartitionMetadata metadata = state.getPartitionMetadata(TaskNaming.stepName(TestStepOne.class));
        Assertions.assertNull(metadata);
    }

    @Test
    public void testGetPartitionMetadata_NoMetadataMarker() {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        WorkflowState state = history.buildCurrentState();

        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        PartitionMetadata metadata = state.getPartitionMetadata(stepName);
        Assertions.assertNull(metadata);
    }

    @Test
    public void testGetPartitionMetadata_ValidMetadataMarker() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(partitionIds);

        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        history.recordPartitionMetadataMarkers(clock.instant(), stepName, partitionIdGeneratorResult);

        WorkflowState state = history.buildCurrentState();

        PartitionMetadata metadata = state.getPartitionMetadata(stepName);
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
    }

    @Test
    public void testGetPartitionMetadata_PartitionIdsSplitAcrossMultipleMetadataMarkers() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(partitionIds);

        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        List<HistoryEvent> markerEvents = history.recordPartitionMetadataMarkers(clock.instant(), stepName, partitionIdGeneratorResult);
        Assertions.assertTrue(markerEvents.size() > 1);

        WorkflowState state = history.buildCurrentState();

        PartitionMetadata metadata = state.getPartitionMetadata(stepName);
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
    }

    @Test
    public void testGetPartitionMetadata_InvalidMetadataMarker() {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        String metadataMarkerName = TaskNaming.partitionMetadataMarkerName(stepName, 0, 1);

        // this marker's content isn't valid so we should behave as if there is no marker
        history.recordMarker(clock.instant(), metadataMarkerName, "this is not valid json {");

        WorkflowState state = history.buildCurrentState();

        PartitionMetadata metadata = state.getPartitionMetadata(stepName);
        Assertions.assertNull(metadata);
    }

    @Test
    public void testGetPartitionMetadata_RespectsLatestMarkerEvent() throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);

        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, clock, Collections.emptyMap());

        clock.forward(Duration.ofMillis(100));
        history.scheduleStepAttempt();

        clock.forward(Duration.ofMillis(100));
        history.recordActivityResult(StepResult.success());

        clock.forward(Duration.ofMillis(100));
        String stepName = TaskNaming.stepName(TestPartitionedStep.class);
        String metadataMarkerName = TaskNaming.partitionMetadataMarkerName(stepName, 0, 1);

        // this marker's content isn't valid, but we're going to add another valid marker afterward
        history.recordMarker(clock.instant(), metadataMarkerName, "this is not valid json {");

        clock.forward(Duration.ofMillis(100));
        PartitionIdGeneratorResult partitionIdGeneratorResult
                = PartitionIdGeneratorResult.create().withPartitionIds(partitionIds);

        history.recordPartitionMetadataMarkers(clock.instant(), stepName, partitionIdGeneratorResult);

        WorkflowState state = history.buildCurrentState();

        PartitionMetadata metadata = state.getPartitionMetadata(stepName);
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
    }

    private void verifyStepResult(WorkflowState ws, String activityName, String partitionId, long partitionCount,
                                  int numAttempts, Instant latestAttemptStartTime, Map<String, String> initialInput,
                                  Map<String, String> finalOutput, StepResult.ResultAction finalResultAction,
                                  String finalResultCode) {
        verifyStepResult(ws, activityName, partitionId, partitionCount, numAttempts, latestAttemptStartTime, initialInput,
                         finalOutput, finalResultAction, finalResultCode, null);
    }

    private void verifyStepResult(WorkflowState ws, String activityName, String partitionId, long partitionCount,
                                  int numAttempts, Instant latestAttemptStartTime, Map<String, String> initialInput,
                                  Map<String, String> finalOutput, StepResult.ResultAction finalResultAction,
                                  String finalResultCode, String finalCompletionMessage) {
        Assertions.assertNotNull(ws.getLatestPartitionStates(activityName));
        Assertions.assertNotNull(ws.getLatestPartitionStates(activityName).get(partitionId));

        PartitionState lastAttempt = ws.getLatestPartitionStates(activityName).get(partitionId);

        Assertions.assertEquals(latestAttemptStartTime, lastAttempt.getAttemptScheduledTime());
        Map<String, String> attemptInput = new HashMap<>(initialInput);
        attemptInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(ws.getWorkflowId()));
        attemptInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(ws.getWorkflowRunId()));
        attemptInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(ws.getWorkflowStartDate()));
        if (numAttempts > 1) {
            attemptInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(numAttempts - 1));
        }
        if(partitionId != null) {
            attemptInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            attemptInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionCount));
        }
        Assertions.assertEquals(attemptInput, lastAttempt.getAttemptInput());

        Map<String, String> attemptOutput = new HashMap<>(finalOutput);
        if (finalResultAction != StepResult.ResultAction.RETRY) {
            attemptOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, finalCompletionMessage);
            attemptOutput.put(StepAttributes.RESULT_CODE, finalResultCode);
        }

        Assertions.assertEquals(attemptOutput, lastAttempt.getAttemptOutput());
        Assertions.assertEquals(finalResultAction, lastAttempt.getAttemptResult());
        Assertions.assertEquals(finalResultCode, lastAttempt.getResultCode());
        Assertions.assertEquals(numAttempts - 1, (int)lastAttempt.getRetryAttempt());
    }

}
