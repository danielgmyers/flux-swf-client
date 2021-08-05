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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.FluxCapacitorImpl;
import software.amazon.aws.clients.swf.flux.poller.signals.DelayRetrySignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.ForceResultSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.SignalType;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestBranchingWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPartitionedStep;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepOne;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepTwo;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithFailureTransition;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithPartitionedStep;
import software.amazon.aws.clients.swf.flux.poller.timers.TimerData;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.services.swf.model.ActivityTaskScheduledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.ChildWorkflowExecutionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

public class WorkflowStateTest {

    @Test
    public void testBuild_ThrowsWithEmptyHistoryList() {
        try {
            Workflow workflow = new TestWorkflow();
            WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, Instant.now());
            PollForDecisionTaskResponse task = history.buildDecisionTask();
            WorkflowState.build(task.toBuilder().events(Collections.emptyList()).build());
            Assert.fail();
        } catch(BadWorkflowStateException e) {
            // expected
        }
    }

    @Test
    public void testBuild_ThrowsIfNoWorkflowStartedEvent() {
        try {
            Workflow workflow = new TestWorkflow();
            WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, Instant.now());
            history.scheduleStepAttempt();
            history.recordActivityResult(StepResult.success());

            PollForDecisionTaskResponse task = history.buildDecisionTask();
            List<HistoryEvent> eventsCopy = new ArrayList<>(task.events());
            Assert.assertEquals(EventType.WORKFLOW_EXECUTION_STARTED, eventsCopy.get(eventsCopy.size()-1).eventType());
            eventsCopy.remove(eventsCopy.size()-1); // we'll remove the last event, which was the workflow started event

            WorkflowState.build(task.toBuilder().events(eventsCopy).build());
            Assert.fail();
        } catch(BadWorkflowStateException e) {
            // expected
        }
    }

    @Test
    public void testBuild_WorkflowStarted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertNull(ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertTrue(ws.getStepPartitions().isEmpty());
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

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent endEvent = history.recordActivityResult(StepResult.success().withAttributes(output));
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(endEvent.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
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

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        HistoryEvent endEvent = history.recordActivityResult(StepResult.failure().withAttributes(output));
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(endEvent.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
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

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        String completionMessage = "Custom result!";
        HistoryEvent endEvent = history.recordActivityResult(StepResult.success(TestBranchingWorkflow.CUSTOM_RESULT, completionMessage).withAttributes(output));
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(endEvent.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertEquals(TestBranchingWorkflow.CUSTOM_RESULT, ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
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

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        String completionMessage = "Custom result!";
        HistoryEvent endEvent = history.recordActivityResult(StepResult.complete(TestBranchingWorkflow.CUSTOM_RESULT, completionMessage).withAttributes(output));

        // we don't use history.recordForcedResultSignal since it helpfully signals the second step of the workflow,
        // because the first step completed successfully.
        // note we use retry attempt 1 even though there isn't an open timer, because WorkflowState only respects signals with
        // retry number = (last attempt + 1)
        ForceResultSignalData signal = new ForceResultSignalData();
        signal.setActivityId(TaskNaming.createActivityId(TestStepOne.class.getSimpleName(), 1, null));
        signal.setResultCode(StepResult.SUCCEED_RESULT_CODE);
        HistoryEvent signalEvent = history.recordSignalEvent(signal);

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(signalEvent.eventTimestamp(), ws.getCurrentStepCompletionTime()); // the signal event's timestamp counts here
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        // we evaluate the step result with the custom result code because the signal doesn't retroactively change the output
        // contents from the activity completion event.
        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
                         input, output, StepResult.ResultAction.COMPLETE, TestBranchingWorkflow.CUSTOM_RESULT, completionMessage);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionActivitiesRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_OnePartitionSucceededAndOneRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p1", StepResult.success());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_OnePartitionFailedAndOneRetried() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p1", StepResult.failure());
        history.recordActivityResult("p2", StepResult.retry());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_PartitionedStep_FirstPartitionSucceededAndSecondFailedToSchedule() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        history.recordScheduleAttemptFailed("p2");

        history.recordActivityResult("p1", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p1"));
        Assert.assertEquals(1, ws.getStepPartitions().get(partitionedStepName).get("p1").size());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p2"));
        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).get("p2").isEmpty());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_FirstFailedToScheduleAndSecondSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        history.recordScheduleAttemptFailed("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startP2Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p1"));
        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).get("p1").isEmpty());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p2"));
        Assert.assertEquals(1, ws.getStepPartitions().get(partitionedStepName).get("p2").size());

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
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

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent step1Start = history.scheduleStepAttempt();
        HistoryEvent step1End = history.recordActivityResult(StepResult.success());

        history.recordScheduleAttemptFailed("p1");
        history.recordScheduleAttemptFailed("p2");

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstStepName, ws.getCurrentActivityName());
        Assert.assertEquals(step1Start.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(step1End.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p1"));
        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).get("p1").isEmpty());

        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).containsKey("p2"));
        Assert.assertTrue(ws.getStepPartitions().get(partitionedStepName).get("p2").isEmpty());
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompleted_BothSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p1", StepResult.success());
        HistoryEvent closeP2Event = history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(closeP2Event.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompleted_OneFailed_OneSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent startP1Event = history.scheduleStepAttempt("p1");
        HistoryEvent startP2Event = history.scheduleStepAttempt("p2");

        history.recordActivityResult("p1", StepResult.failure());
        HistoryEvent closeP2Event = history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(startP1Event.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(closeP2Event.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Collections.singletonList(startP1Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Collections.singletonList(startP2Event.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompletedAfterMultipleAttempts() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent p1Start1 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start1 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        HistoryEvent p1Timer1 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer1 = history.startRetryTimer("p2", Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        HistoryEvent p1Start2 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start2 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        HistoryEvent p1Timer2 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer2 = history.startRetryTimer("p2", Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        HistoryEvent p1Start3 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start3 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.success());
        HistoryEvent p2End3 = history.recordActivityResult("p2", StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertEquals(4, ws.getClosedTimers().size()); // two retries each for two partitions
        Assert.assertTrue(ws.getClosedTimers().containsKey(p1Timer1.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p2Timer1.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p1Timer2.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p2Timer2.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(p1Start1.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(p2End3.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(2L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Arrays.asList(p1Start1.eventTimestamp(), p1Start2.eventTimestamp(), p1Start3.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Arrays.asList(p2Start1.eventTimestamp(), p2Start2.eventTimestamp(), p2Start3.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_PartitionedStep_BothPartitionsCompletedAfterMultipleAttempts_OneFailed_OneSucceeded() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        List<String> partitionIds = Arrays.asList("p1", "p2");

        Workflow workflow = new TestWorkflowWithPartitionedStep(partitionIds);
        String partitionedStepName = TaskNaming.activityName(workflow, TestPartitionedStep.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent p1Start1 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start1 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        HistoryEvent p1Timer1 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer1 = history.startRetryTimer("p2", Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        HistoryEvent p1Start2 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start2 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.retry());
        history.recordActivityResult("p2", StepResult.retry());

        HistoryEvent p1Timer2 = history.startRetryTimer("p1", Duration.ofSeconds(10));
        HistoryEvent p2Timer2 = history.startRetryTimer("p2", Duration.ofSeconds(10));
        history.closeRetryTimer("p1", false);
        history.closeRetryTimer("p2", false);

        HistoryEvent p1Start3 = history.scheduleStepAttempt("p1");
        HistoryEvent p2Start3 = history.scheduleStepAttempt("p2");
        history.recordActivityResult("p1", StepResult.success());
        HistoryEvent p2End3 = history.recordActivityResult("p2", StepResult.failure());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertEquals(4, ws.getClosedTimers().size()); // two retries each for two partitions
        Assert.assertTrue(ws.getClosedTimers().containsKey(p1Timer1.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p2Timer1.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p1Timer2.timerStartedEventAttributes().timerId()));
        Assert.assertTrue(ws.getClosedTimers().containsKey(p2Timer2.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(partitionedStepName, ws.getCurrentActivityName());
        // the effectiveResultCode should be _fail as one of the partitions has failed
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(p1Start1.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(p2End3.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(2L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(partitionedStepName));
        Assert.assertEquals(2, ws.getStepPartitions().get(partitionedStepName).size());

        verifyStepResult(ws, partitionedStepName, "p1", 2, Arrays.asList(p1Start1.eventTimestamp(), p1Start2.eventTimestamp(), p1Start3.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, partitionedStepName, "p2", 2, Arrays.asList(p2Start1.eventTimestamp(), p2Start2.eventTimestamp(), p2Start3.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);
    }

    @Test
    public void testBuild_ActivityTimedOut_Retry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        history.recordActivityTimedOut();
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_ActivityCanceled_Retry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent startEvent = history.scheduleStepAttempt();
        history.recordActivityCanceled();
        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(startEvent.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(startEvent.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.RETRY, null);
    }

    @Test
    public void testBuild_MultipleStepAttemptsPresent_Success() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        history.closeRetryTimer(false);

        HistoryEvent secondStart = history.scheduleStepAttempt();
        HistoryEvent secondClose = history.recordActivityResult(StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertEquals(1, ws.getClosedTimers().size());
        Assert.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(secondClose.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(1L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertFalse(ws.getStepPartitions().isEmpty());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Arrays.asList(firstStart.eventTimestamp(), secondStart.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_UsesMostRecentStepWhenMultipleStepsInHistory_Success() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent secondStart = history.scheduleStepAttempt();
        HistoryEvent secondClose = history.recordActivityResult(StepResult.success());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(secondStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(secondClose.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        Assert.assertNotNull(ws.getStepPartitions().get(secondActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(secondActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(firstStart.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, Collections.singletonList(secondStart.eventTimestamp()),
                         input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsOpenTimer() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertEquals(1, ws.getOpenTimers().size());
        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(new TimerData(timerStart), ws.getOpenTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_DetectsTimerFired() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent timerFired = history.closeRetryTimer(false);

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());

        Assert.assertEquals(1, ws.getClosedTimers().size());
        Assert.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(timerFired.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

    }

    @Test
    public void testBuild_DetectsTimerCancelled() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent timerCancelled = history.closeRetryTimer(true);

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());

        Assert.assertEquals(1, ws.getClosedTimers().size());
        Assert.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(timerCancelled.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_DetectsTimerThatWasCancelledAndRestarted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent timerCancelled = history.closeRetryTimer(true);

        HistoryEvent timerRestart = history.startRetryTimer(Duration.ofSeconds(10));
        Assert.assertEquals(timerStart.timerStartedEventAttributes().timerId(), timerRestart.timerStartedEventAttributes().timerId());

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerRestart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_DetectsTimerThatWasCancelledAndRestartedAndFiredNormally() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent timerCancelled = history.closeRetryTimer(true);

        HistoryEvent timerRestart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent timerFired = history.closeRetryTimer(false);

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getOpenTimers().isEmpty());

        Assert.assertTrue(ws.getClosedTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(timerFired.eventId(), ws.getClosedTimers().get(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, ws.getCurrentStepMaxRetryCount().longValue());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_StoresRetryNowSignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent signal = history.recordRetryNowSignal();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assert.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_StoresRetryNowSignal_EvenIfSignalDataContainsUnknownFields() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        Map<String, String> rawSignalData = new HashMap<>();
        rawSignalData.put("activityId", timerStart.timerStartedEventAttributes().timerId());
        rawSignalData.put("someIrrelevantOperationalField", "Signal Sent By jtkirk");
        HistoryEvent signal = history.recordSignalEvent(SignalType.RETRY_NOW.getFriendlyName(), new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assert.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_IgnoresRetryNowSignalWhenInputDataIsMissingFields() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        Map<String, String> rawSignalData = new HashMap<>();
        // intentionally not populating activity id
        history.recordSignalEvent(SignalType.RETRY_NOW.getFriendlyName(), new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_StoresDelayRetrySignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent signal = history.recordDelayRetrySignal(Duration.ofSeconds(142));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.DELAY_RETRY, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assert.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assert.assertEquals(142, ((DelayRetrySignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getDelayInSeconds().intValue());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_StoresScheduleDelayedRetrySignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        history.recordDelayRetrySignal(Duration.ofSeconds(142));
        HistoryEvent signal2 = history.recordScheduleDelayedRetrySignal(Duration.ofSeconds(142));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.SCHEDULE_DELAYED_RETRY, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assert.assertEquals(signal2.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assert.assertEquals(142, ((DelayRetrySignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getDelayInSeconds().intValue());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_ForceResultSignalActuallyForcesResult() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent signal = history.recordForceResultSignal(StepResult.FAIL_RESULT_CODE);

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.FORCE_RESULT, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        Assert.assertEquals(signal.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ((ForceResultSignalData)ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId())).getResultCode());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(WorkflowState.FORCED_RESULT_MESSAGE, ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(signal.eventTimestamp(), ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_OnlyConsidersLatestSignal() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));
        HistoryEvent signal = history.recordRetryNowSignal();
        HistoryEvent signal2 = history.recordRetryNowSignal(); // same signal, repeated

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().containsKey(timerStart.timerStartedEventAttributes().timerId()));
        Assert.assertEquals(SignalType.RETRY_NOW, ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalType());
        // signal2 was second so it's the event that should be retained
        Assert.assertEquals(signal2.eventId(), ws.getSignalsByActivityId().get(timerStart.timerStartedEventAttributes().timerId()).getSignalEventId());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_IgnoresInvalidSignalType() throws JsonProcessingException {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        Map<String, String> rawSignalData = new HashMap<>();
        rawSignalData.put("activityId", timerStart.timerStartedEventAttributes().timerId());
        history.recordSignalEvent("fake signal type", new ObjectMapper().writeValueAsString(rawSignalData));

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_IgnoresInvalidSignalData() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent firstStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());

        HistoryEvent timerStart = history.startRetryTimer(Duration.ofSeconds(10));

        history.recordSignalEvent("fake signal type", "this is not valid json");

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());

        Assert.assertTrue(ws.getSignalsByActivityId().isEmpty());

        Assert.assertTrue(ws.getOpenTimers().containsKey(timerStart.timerStartedEventAttributes().timerId()));

        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepResultCode());
        Assert.assertNull(ws.getCurrentStepLastActivityCompletionMessage());
        Assert.assertEquals(firstStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepCompletionTime());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_DetectsWorkflowCancelRequest() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent cancelEvent = history.recordCancelWorkflowExecutionRequest();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertTrue(ws.isWorkflowCancelRequested());
        Assert.assertEquals(cancelEvent.eventTimestamp(), ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertNull(ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertTrue(ws.getStepPartitions().isEmpty());
    }

    @Test
    public void testBuild_DetectsWorkflowCancelRequest_FirstStepInProgress() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent stepStart = history.scheduleStepAttempt();

        HistoryEvent cancelEvent = history.recordCancelWorkflowExecutionRequest();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertTrue(ws.isWorkflowCancelRequested());
        Assert.assertEquals(cancelEvent.eventTimestamp(), ws.getWorkflowCancelRequestDate());
        Assert.assertFalse(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(firstActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(stepStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(0, (long)ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(1, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionCanceled() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);
        HistoryEvent cancelRequestEvent = history.recordCancelWorkflowExecutionRequest();
        HistoryEvent cancelEvent = history.recordWorkflowCanceled();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertTrue(ws.isWorkflowCancelRequested());
        Assert.assertEquals(cancelRequestEvent.eventTimestamp(), ws.getWorkflowCancelRequestDate());
        Assert.assertTrue(ws.isWorkflowExecutionClosed());

        Assert.assertNull(ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertTrue(ws.getStepPartitions().isEmpty());
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionCompleted() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent stepOneStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent stepTwoStart = history.scheduleStepAttempt();
        HistoryEvent stepTwoEnd = history.recordActivityResult(StepResult.success());

        history.recordWorkflowCompleted();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertTrue(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.SUCCEED_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(stepTwoStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(stepTwoEnd.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        Assert.assertNotNull(ws.getStepPartitions().get(secondActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(secondActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(stepOneStart.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, Collections.singletonList(stepTwoStart.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionFailed() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflowWithFailureTransition();
        String firstActivityName = TaskNaming.activityName(workflow, TestStepOne.class);
        String secondActivityName = TaskNaming.activityName(workflow, TestStepTwo.class);

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        HistoryEvent stepOneStart = history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());

        HistoryEvent stepTwoStart = history.scheduleStepAttempt();
        HistoryEvent stepTwoEnd = history.recordActivityResult(StepResult.failure());

        history.recordWorkflowFailed();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertTrue(ws.isWorkflowExecutionClosed());

        Assert.assertEquals(secondActivityName, ws.getCurrentActivityName());
        Assert.assertEquals(StepResult.FAIL_RESULT_CODE, ws.getCurrentStepResultCode());
        Assert.assertEquals(stepTwoStart.eventTimestamp(), ws.getCurrentStepFirstScheduledTime());
        Assert.assertEquals(stepTwoEnd.eventTimestamp(), ws.getCurrentStepCompletionTime());
        Assert.assertEquals(0L, (long)ws.getCurrentStepMaxRetryCount());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertEquals(2, ws.getStepPartitions().size());

        Assert.assertNotNull(ws.getStepPartitions().get(firstActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(firstActivityName).size());

        Assert.assertNotNull(ws.getStepPartitions().get(secondActivityName));
        Assert.assertEquals(1, ws.getStepPartitions().get(secondActivityName).size());

        verifyStepResult(ws, firstActivityName, null, 1, Collections.singletonList(stepOneStart.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE);

        verifyStepResult(ws, secondActivityName, null, 1, Collections.singletonList(stepTwoStart.eventTimestamp()),
                input, Collections.emptyMap(), StepResult.ResultAction.COMPLETE, StepResult.FAIL_RESULT_CODE);
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionTerminated() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.recordWorkflowTerminated();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertTrue(ws.isWorkflowExecutionClosed());

        Assert.assertNull(ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertTrue(ws.getStepPartitions().isEmpty());
    }

    @Test
    public void testBuild_DetectsWorkflowCompletion_ExecutionTimedOut() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        Workflow workflow = new TestWorkflow();

        Instant now = Instant.now();
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(workflow, now, input);

        history.recordWorkflowTimedOut();

        WorkflowState ws = history.buildCurrentState();

        Assert.assertEquals(now, ws.getWorkflowStartDate());
        Assert.assertEquals(input, ws.getWorkflowInput());
        Assert.assertTrue(ws.getOpenTimers().isEmpty());
        Assert.assertTrue(ws.getClosedTimers().isEmpty());

        Assert.assertFalse(ws.isWorkflowCancelRequested());
        Assert.assertNull(ws.getWorkflowCancelRequestDate());
        Assert.assertTrue(ws.isWorkflowExecutionClosed());

        Assert.assertNull(ws.getCurrentActivityName());
        Assert.assertNull(ws.getCurrentStepFirstScheduledTime());
        Assert.assertNull(ws.getCurrentStepMaxRetryCount());
        Assert.assertNull(ws.getCurrentStepResultCode());

        Assert.assertNotNull(ws.getStepPartitions());
        Assert.assertTrue(ws.getStepPartitions().isEmpty());
    }

    @Test
    public void testGetStepData_WorkflowStartedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
    }

    @Test
    public void testGetStepData_ActivityScheduledEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        history.scheduleStepAttempt();
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
    }

    @Test
    public void testGetStepData_ActivityCompletedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.success());
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
    }

    @Test
    public void testGetStepData_ActivityTimedOutEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        history.scheduleStepAttempt();
        history.recordActivityTimedOut();
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
    }

    @Test
    public void testGetStepData_ActivityFailedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry());
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
    }

    @Test
    public void testGetStepData_ActivityFailedViaException() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        history.scheduleStepAttempt();
        history.recordActivityResult(StepResult.retry(new RuntimeException()));
        PollForDecisionTaskResponse task = history.buildDecisionTask();

        // the first event in the list is the most recent
        Assert.assertNotNull(WorkflowState.getStepData(task.events().get(0)));
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
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetStepName_WorkflowStartedEvent() {
        WorkflowHistoryBuilder history = WorkflowHistoryBuilder.startWorkflow(new TestWorkflow(), Instant.now());
        PollForDecisionTaskResponse task = history.buildDecisionTask();
        // the first event in the list is the most recent
        Assert.assertNull(WorkflowState.getActivityName(task.events().get(0)));
    }

    @Test
    public void testGetStepName_ActivityScheduledEvent() {
        String stepName = TaskNaming.activityName(TestWorkflow.class, TestStepOne.class);
        ActivityType type = ActivityType.builder().name(stepName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();

        ActivityTaskScheduledEventAttributes attrs = ActivityTaskScheduledEventAttributes.builder().activityType(type).build();

        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_SCHEDULED)
                .activityTaskScheduledEventAttributes(attrs).build();

        Assert.assertEquals(stepName, WorkflowState.getActivityName(event));
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
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetStepOutput_ActivityCompletedEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_COMPLETED).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assert.assertEquals(StepResult.ResultAction.COMPLETE, result);
    }

    @Test
    public void testGetStepResultAction_ActivityTimedOutEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_TIMED_OUT).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assert.assertEquals(StepResult.ResultAction.RETRY, result);
    }

    @Test
    public void testGetStepResultAction_ActivityFailedEvent() {
        HistoryEvent event = HistoryEvent.builder().eventType(EventType.ACTIVITY_TASK_FAILED).build();
        StepResult.ResultAction result = WorkflowState.getStepResultAction(event);
        Assert.assertEquals(StepResult.ResultAction.RETRY, result);
    }

    @Test
    public void testGetStepResultAction_ThrowsForOtherEvent() {
        try {
            HistoryEvent event = HistoryEvent.builder().eventType(EventType.CHILD_WORKFLOW_EXECUTION_COMPLETED).build();
            WorkflowState.getStepResultAction(event);
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    private void verifyStepResult(WorkflowState ws, String stepName, String partitionId, long partitionCount,
                                  List<Instant> attemptStartTimes, Map<String, String> initialInput,
                                  Map<String, String> finalOutput, StepResult.ResultAction finalResultAction, String finalResultCode) {
        verifyStepResult(ws, stepName, partitionId, partitionCount, attemptStartTimes, initialInput, finalOutput, finalResultAction, finalResultCode, null);
    }

    private void verifyStepResult(WorkflowState ws, String stepName, String partitionId, long partitionCount,
                                  List<Instant> attemptStartTimes, Map<String, String> initialInput,
                                  Map<String, String> finalOutput, StepResult.ResultAction finalResultAction, String finalResultCode,
                                  String finalCompletionMessage) {
        Assert.assertTrue(ws.getStepPartitions().containsKey(stepName));
        Assert.assertNotNull(ws.getStepPartitions().get(stepName));
        Assert.assertEquals(partitionCount, ws.getStepPartitions().get(stepName).size());
        Assert.assertNotNull(ws.getStepPartitions().get(stepName).get(partitionId));
        Assert.assertEquals(attemptStartTimes.size(), ws.getStepPartitions().get(stepName).get(partitionId).size());

        for (int i = 0; i < attemptStartTimes.size(); i++) {
            PartitionState attempt = ws.getStepPartitions().get(stepName).get(partitionId).get(i);

            Assert.assertEquals(attemptStartTimes.get(i), attempt.getAttemptScheduledTime());
            Map<String, String> attemptInput = new HashMap<>(initialInput);
            attemptInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(ws.getWorkflowId()));
            attemptInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(ws.getWorkflowRunId()));
            attemptInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(ws.getWorkflowStartDate()));
            if (i > 0) {
                attemptInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(i));
            }
            if(partitionId != null) {
                attemptInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
                attemptInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionCount));
            }
            Assert.assertEquals(attemptInput, attempt.getAttemptInput());

            Map<String, String> attemptOutput = new HashMap<>();
            StepResult.ResultAction attemptResultAction = StepResult.ResultAction.RETRY;
            String attemptResultCode = null;
            if (i == attemptStartTimes.size()-1) {
                attemptOutput.putAll(finalOutput);
                attemptResultAction = finalResultAction;
                attemptResultCode = finalResultCode;
            }

            if (attemptResultAction != StepResult.ResultAction.RETRY) {
                attemptOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, finalCompletionMessage);
                attemptOutput.put(StepAttributes.RESULT_CODE, finalResultCode);
            }

            Assert.assertEquals(attemptOutput, attempt.getAttemptOutput());
            Assert.assertEquals(attemptResultAction, attempt.getAttemptResult());
            Assert.assertEquals(attemptResultCode, attempt.getResultCode());
            Assert.assertEquals(i, attempt.getRetryAttempt());
        }
    }

}
