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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

import software.amazon.aws.clients.swf.flux.FluxCapacitorImpl;
import software.amazon.aws.clients.swf.flux.metrics.NoopMetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.poller.signals.BaseSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.DelayRetrySignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.ForceResultSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.RetryNowSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.ScheduleDelayedRetrySignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.SignalUtils;
import software.amazon.aws.clients.swf.flux.step.PartitionIdGeneratorResult;
import software.amazon.aws.clients.swf.flux.step.PartitionedWorkflowStep;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepUtil;
import software.amazon.aws.clients.swf.flux.wf.Periodic;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphNode;
import software.amazon.awssdk.services.swf.model.ActivityTaskCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskScheduledEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.ActivityTaskTimeoutType;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.MarkerRecordedEventAttributes;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskFailedCause;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TimerCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerFiredEventAttributes;
import software.amazon.awssdk.services.swf.model.TimerStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionCancelRequestedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionCanceledEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionCompletedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionFailedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionSignaledEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionStartedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionTerminatedEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionTimedOutEventAttributes;
import software.amazon.awssdk.services.swf.model.WorkflowType;

public class WorkflowHistoryBuilder {

    static final String TASK_TOKEN = "task-token";
    static final String WORKFLOW_ID = "workflow-id";
    static final String RUN_ID = "run-id";

    private static class EventIdVendor {
        private long nextEventId = 1;
        long next() {
            return nextEventId++;
        }
    }

    private final List<HistoryEvent> events;
    private final Workflow workflow;
    private final EventIdVendor eventIds;

    private final WorkflowType workflowType;

    private WorkflowStep currentStep;
    private boolean currentStepIsPartitioned;
    private final Map<String, Long> currentStepPartitionRetryAttempt;
    private final Map<String, HistoryEvent> currentStepPartitionLastScheduledEvent;
    private final Map<String, String> currentStepInput;
    private final Map<String, StepResult> currentStepResults;
    private final Map<String, HistoryEvent> currentStepOpenTimers;

    private WorkflowHistoryBuilder(Workflow workflow, Instant workflowStartTime,
                                   Map<String, String> input) {
        this.events = new ArrayList<>();
        this.workflow = workflow;
        this.eventIds = new EventIdVendor();

        this.currentStepPartitionLastScheduledEvent = new HashMap<>();
        this.currentStepInput = new HashMap<>();
        if (input != null) {
            this.currentStepInput.putAll(input);
        }
        currentStepInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(WORKFLOW_ID));
        currentStepInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(RUN_ID));
        currentStepInput.put(StepAttributes.WORKFLOW_START_TIME, StepAttributes.encode(Date.from(workflowStartTime)));

        currentStepPartitionRetryAttempt = new HashMap<>();
        this.currentStepResults = new HashMap<>();
        this.currentStepOpenTimers = new HashMap<>();

        this.workflowType = WorkflowType.builder().name(TaskNaming.workflowName(workflow))
                                                  .version(FluxCapacitorImpl.WORKFLOW_VERSION).build();

        WorkflowExecutionStartedEventAttributes attrs = WorkflowExecutionStartedEventAttributes.builder()
                .workflowType(workflowType)
                .input(StepAttributes.encode(input))
                .taskList(TaskList.builder().name(workflow.taskList()).build())
                .childPolicy(ChildPolicy.TERMINATE)
                .executionStartToCloseTimeout(Long.toString(workflow.maxStartToCloseDuration().getSeconds()))
                .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                .build();

        HistoryEvent event = HistoryEvent.builder().eventTimestamp(workflowStartTime).eventId(eventIds.next())
                .eventType(EventType.WORKFLOW_EXECUTION_STARTED).workflowExecutionStartedEventAttributes(attrs).build();
        events.add(event);

        prepareStep(workflow.getGraph().getFirstStep());
    }

    public static WorkflowHistoryBuilder startWorkflow(Workflow workflow, Instant workflowStartTime) {
        return startWorkflow(workflow, workflowStartTime, Collections.emptyMap());
    }

    public static WorkflowHistoryBuilder startWorkflow(Workflow workflow, Instant workflowStartTime,
                                                       Map<String, String> input) {
        return new WorkflowHistoryBuilder(workflow, workflowStartTime, input);
    }

    public WorkflowState buildCurrentState() {
        return WorkflowState.build(buildDecisionTask());
    }

    public PollForDecisionTaskResponse buildDecisionTask() {
        List<HistoryEvent> eventsCopy = new ArrayList<>(events);
        Collections.reverse(eventsCopy);
        return PollForDecisionTaskResponse.builder()
                .workflowType(workflowType)
                .workflowExecution(WorkflowExecution.builder().workflowId(WORKFLOW_ID).runId(RUN_ID).build())
                .taskToken(TASK_TOKEN)
                .events(Collections.unmodifiableList(eventsCopy))
                .build();
    }

    public HistoryEvent scheduleStepAttempt() {
        return scheduleStepAttempt(null);
    }

    public HistoryEvent scheduleStepAttempt(String partitionId) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (currentStepResults.containsKey(partitionId)
                && currentStepResults.get(partitionId).getAction() == StepResult.ResultAction.COMPLETE) {
            throw new IllegalStateException();
        }
        if (currentStepOpenTimers.containsKey(partitionId)) {
            throw new IllegalStateException();
        }

        HistoryEvent event = buildActivityScheduledEvent(Instant.now(), partitionId);
        currentStepPartitionLastScheduledEvent.put(partitionId, event);
        events.add(event);
        return event;
    }

    public HistoryEvent recordScheduleAttemptFailed() {
        return recordScheduleAttemptFailed(null);
    }

    public HistoryEvent recordScheduleAttemptFailed(String partitionId) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (currentStepResults.containsKey(partitionId)
                && currentStepResults.get(partitionId).getAction() == StepResult.ResultAction.COMPLETE) {
            throw new IllegalStateException();
        }
        if (currentStepOpenTimers.containsKey(partitionId)) {
            throw new IllegalStateException();
        }

        HistoryEvent event = buildScheduleActivityTaskFailedEvent(Instant.now(), partitionId);
        events.add(event);
        return event;
    }

    public HistoryEvent recordActivityResult(StepResult result) {
        return recordActivityResult(null, result);
    }

    public HistoryEvent recordActivityResult(String partitionId, StepResult result) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (!currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (currentStepResults.containsKey(partitionId)
                && currentStepResults.get(partitionId).getAction() == StepResult.ResultAction.COMPLETE) {
            throw new IllegalStateException();
        }
        if (currentStepOpenTimers.containsKey(partitionId)) {
            throw new IllegalStateException();
        }

        WorkflowGraphNode currentStepNode = workflow.getGraph().getNodes().get(currentStep.getClass());
        currentStepResults.put(partitionId, result);

        HistoryEvent scheduledEvent = currentStepPartitionLastScheduledEvent.get(partitionId);

        HistoryEvent resultEvent;
        if (result.getAction() == StepResult.ResultAction.RETRY) {
            if (!result.getAttributes().isEmpty()) {
                throw new IllegalArgumentException();
            }

            resultEvent = buildActivityFailedEvent(Instant.now(), result, scheduledEvent);

            long retryAttempt = currentStepPartitionRetryAttempt.get(partitionId);
            retryAttempt++;
            currentStepPartitionRetryAttempt.put(partitionId, retryAttempt);

            currentStepPartitionLastScheduledEvent.remove(partitionId);
        } else {
            if (currentStepIsPartitioned && !result.getAttributes().isEmpty()) {
                throw new IllegalArgumentException();
            }

            Map<String, String> outputAttrs = new HashMap<>();
            for (Map.Entry<String, Object> attribute : result.getAttributes().entrySet()) {
                outputAttrs.put(attribute.getKey(), attribute.getValue().toString());
            }
            outputAttrs.put(StepAttributes.RESULT_CODE, result.getResultCode());
            outputAttrs.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());

            resultEvent = buildActivityCompletedEvent(Instant.now(), outputAttrs, scheduledEvent);
        }

        long completedPartitions = currentStepResults.values().stream().filter(r -> r.getAction() == StepResult.ResultAction.COMPLETE).count();
        if (completedPartitions == currentStepPartitionRetryAttempt.size()) {
            String stepResultToFollow;
            if (currentStepNode.getNextStepsByResultCode().containsKey(StepResult.ALWAYS_RESULT_CODE)) {
                stepResultToFollow = StepResult.ALWAYS_RESULT_CODE;
            } else if (currentStepIsPartitioned) {
                boolean anyFailed = currentStepResults.values().stream().anyMatch(r -> StepResult.FAIL_RESULT_CODE.equals(r.getResultCode()));
                if (anyFailed) {
                    stepResultToFollow = StepResult.FAIL_RESULT_CODE;
                } else {
                    stepResultToFollow = StepResult.SUCCEED_RESULT_CODE;
                }
            } else {
                stepResultToFollow = result.getResultCode();
            }


            if (currentStepNode.getNextStepsByResultCode().containsKey(stepResultToFollow)) {
                currentStepNode = currentStepNode.getNextStepsByResultCode().get(stepResultToFollow);
                if (currentStepNode == null) {
                    prepareStep(null);
                } else {
                    prepareStep(currentStepNode.getStep());
                }
            }
        }

        events.add(resultEvent);
        return resultEvent;
    }

    public HistoryEvent recordActivityTimedOut() {
        return recordActivityTimedOut(null);
    }

    public HistoryEvent recordActivityTimedOut(String partitionId) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (!currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (currentStepResults.containsKey(partitionId)
                && currentStepResults.get(partitionId).getAction() == StepResult.ResultAction.COMPLETE) {
            throw new IllegalStateException();
        }
        if (currentStepOpenTimers.containsKey(partitionId)) {
            throw new IllegalStateException();
        }

        HistoryEvent scheduledEvent = currentStepPartitionLastScheduledEvent.get(partitionId);
        currentStepPartitionLastScheduledEvent.remove(partitionId);

        long retryAttempt = currentStepPartitionRetryAttempt.get(partitionId);
        retryAttempt++;
        currentStepPartitionRetryAttempt.put(partitionId, retryAttempt);

        ActivityTaskTimedOutEventAttributes attrs = ActivityTaskTimedOutEventAttributes.builder()
                .scheduledEventId(scheduledEvent.eventId())
                .timeoutType(ActivityTaskTimeoutType.HEARTBEAT)
                .build();

        HistoryEvent event = HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(Instant.now())
                .eventType(EventType.ACTIVITY_TASK_TIMED_OUT).activityTaskTimedOutEventAttributes(attrs).build();
        events.add(event);

        return event;
    }

    public HistoryEvent recordActivityCanceled() {
        return recordActivityCanceled(null);
    }

    public HistoryEvent recordActivityCanceled(String partitionId) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (!currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (currentStepResults.containsKey(partitionId)
                && currentStepResults.get(partitionId).getAction() == StepResult.ResultAction.COMPLETE) {
            throw new IllegalStateException();
        }
        if (currentStepOpenTimers.containsKey(partitionId)) {
            throw new IllegalStateException();
        }

        HistoryEvent scheduledEvent = currentStepPartitionLastScheduledEvent.get(partitionId);
        currentStepPartitionLastScheduledEvent.remove(partitionId);

        long retryAttempt = currentStepPartitionRetryAttempt.get(partitionId);
        retryAttempt++;
        currentStepPartitionRetryAttempt.put(partitionId, retryAttempt);

        ActivityTaskCanceledEventAttributes attrs = ActivityTaskCanceledEventAttributes.builder()
                .scheduledEventId(scheduledEvent.eventId()).build();

        HistoryEvent event = HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(Instant.now())
                .eventType(EventType.ACTIVITY_TASK_CANCELED).activityTaskCanceledEventAttributes(attrs).build();
        events.add(event);

        return event;
    }

    private void prepareStep(WorkflowStep nextStep) {
        if (!currentStepIsPartitioned && currentStepResults.containsKey(null)) {
            for (Map.Entry<String, Object> attribute : currentStepResults.get(null).getAttributes().entrySet()) {
                currentStepInput.put(attribute.getKey(), (String)attribute.getValue());
            }
        }

        currentStep = nextStep;
        currentStepPartitionLastScheduledEvent.clear();
        currentStepPartitionRetryAttempt.clear();
        currentStepOpenTimers.clear();
        currentStepResults.clear();

        if (nextStep == null) {
            currentStepIsPartitioned = false;
            currentStepPartitionRetryAttempt.clear();
        } else {
            currentStepIsPartitioned = PartitionedWorkflowStep.class.isAssignableFrom(nextStep.getClass());
            if (!currentStepIsPartitioned) {
                currentStepPartitionRetryAttempt.put(null, 0L);
            } else {
                String workflowName = TaskNaming.workflowName(workflow);
                PartitionedWorkflowStep partitionedStep = (PartitionedWorkflowStep) nextStep;
                PartitionIdGeneratorResult result = WorkflowStepUtil.getPartitionIdsForPartitionedStep(partitionedStep, currentStepInput, workflowName, WORKFLOW_ID, new NoopMetricRecorderFactory());
                if (result.getPartitionIds().isEmpty() || result.getPartitionIds().contains(null)) {
                    throw new IllegalArgumentException();
                }
                result.getPartitionIds().forEach(partitionId -> currentStepPartitionRetryAttempt.put(partitionId, 0L));
                currentStepInput.putAll(StepAttributes.serializeMapValues(result.getAdditionalAttributes()));
            }
        }
    }

    public HistoryEvent startRetryTimer(Duration retryDelay) {
        return startRetryTimer(null, retryDelay);
    }

    public HistoryEvent startRetryTimer(String partitionId, Duration retryDelay) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        if (currentStepPartitionLastScheduledEvent.containsKey(partitionId)) {
            throw new IllegalStateException();
        }
        if (!currentStepResults.containsKey(partitionId)
                || currentStepResults.get(partitionId).getAction() != StepResult.ResultAction.RETRY) {
            throw new IllegalStateException();
        }


        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                                                     currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        return startTimer(timerId, retryDelay);
    }

    public HistoryEvent startTimer(String timerId, Duration retryDelay) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }
        HistoryEvent event = buildTimerStartedEvent(timerId, retryDelay);
        currentStepOpenTimers.put(timerId, event);
        events.add(event);
        return event;
    }

    public HistoryEvent closeRetryTimer(boolean cancelled) {
        return closeRetryTimer(null, cancelled);
    }

    public HistoryEvent closeRetryTimer(String partitionId, boolean cancelled) {
        if (currentStep == null) {
            throw new IllegalStateException("Workflow is over!");
        }

        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        if (!currentStepOpenTimers.containsKey(timerId)) {
            throw new IllegalStateException();
        }

        HistoryEvent event = buildTimerClosedEvent(timerId, cancelled);
        currentStepOpenTimers.remove(timerId);
        events.add(event);
        return event;
    }

    public HistoryEvent startDelayExitTimer() {
        if (currentStep != null) {
            throw new IllegalStateException("The workflow is not over yet!");
        }

        if (!workflow.getClass().isAnnotationPresent(Periodic.class)) {
            throw new IllegalStateException("Non-periodic workflows don't have delay-exit timers.");
        }

        Periodic p = workflow.getClass().getAnnotation(Periodic.class);

        Duration timerDuration = Duration.ofSeconds(p.intervalUnits().toSeconds(p.runInterval()));
        Duration workflowRuntime = Duration.between(events.get(0).eventTimestamp(), Instant.now());

        timerDuration = timerDuration.minus(workflowRuntime);
        if (timerDuration.isZero() || timerDuration.isNegative()) {
            timerDuration = Duration.ofSeconds(1);
        }

        HistoryEvent event = buildTimerStartedEvent(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, timerDuration);
        currentStepOpenTimers.put(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, event);
        events.add(event);
        return event;
    }

    public HistoryEvent closeDelayExitTimer(boolean cancelled) {
        if (!currentStepOpenTimers.containsKey(DecisionTaskPoller.DELAY_EXIT_TIMER_ID)) {
            throw new IllegalStateException();
        }

        HistoryEvent event = buildTimerClosedEvent(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, cancelled);
        currentStepOpenTimers.remove(DecisionTaskPoller.DELAY_EXIT_TIMER_ID);
        events.add(event);
        return event;
    }

    public HistoryEvent recordCancelWorkflowExecutionRequest() {
        HistoryEvent event = buildWorkflowCancelRequestedEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordWorkflowCanceled() {
        HistoryEvent event = buildWorkflowCanceledEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordWorkflowTerminated() {
        HistoryEvent event = buildWorkflowTerminatedEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordWorkflowTimedOut() {
        HistoryEvent event = buildWorkflowTimedOutEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordWorkflowCompleted() {
        if (currentStep != null) {
            throw new IllegalStateException("Workflow is not finished yet!");
        }

        HistoryEvent event = buildWorkflowCompletedEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordWorkflowFailed() {
        if (currentStep != null) {
            throw new IllegalStateException("Workflow is not finished yet!");
        }

        HistoryEvent event = buildWorkflowFailedEvent(Instant.now());
        events.add(event);
        return event;
    }

    public HistoryEvent recordSignalEvent(BaseSignalData signalData) throws JsonProcessingException {
        HistoryEvent event = buildSignalEvent(Instant.now(), signalData);
        events.add(event);
        return event;
    }

    public HistoryEvent recordSignalEvent(String rawSignalType, String rawSignalData) {
        HistoryEvent event = buildSignalEvent(Instant.now(), rawSignalType, rawSignalData);
        events.add(event);
        return event;
    }

    public HistoryEvent recordRetryNowSignal() throws JsonProcessingException {
        return recordRetryNowSignal(null);
    }

    public HistoryEvent recordRetryNowSignal(String partitionId) throws JsonProcessingException {
        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        RetryNowSignalData signal = new RetryNowSignalData();
        signal.setActivityId(timerId);
        return recordSignalEvent(signal);
    }

    public HistoryEvent recordDelayRetrySignal(Duration delay) throws JsonProcessingException {
        return recordDelayRetrySignal(null, delay);
    }

    public HistoryEvent recordDelayRetrySignal(String partitionId, Duration delay) throws JsonProcessingException {
        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        DelayRetrySignalData signal = new DelayRetrySignalData();
        signal.setActivityId(timerId);
        signal.setDelayInSeconds((int)delay.getSeconds());
        return recordSignalEvent(signal);
    }

    public HistoryEvent recordScheduleDelayedRetrySignal(Duration delay) throws JsonProcessingException {
        return recordScheduleDelayedRetrySignal(null, delay);
    }

    public HistoryEvent recordScheduleDelayedRetrySignal(String partitionId, Duration delay) throws JsonProcessingException {
        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        ScheduleDelayedRetrySignalData signal = new ScheduleDelayedRetrySignalData();
        signal.setActivityId(timerId);
        signal.setDelayInSeconds((int)delay.getSeconds());
        return recordSignalEvent(signal);
    }

    public HistoryEvent recordForceResultSignal(String resultCode) throws JsonProcessingException {
        return recordForceResultSignal(null, resultCode);
    }

    public HistoryEvent recordForceResultSignal(String partitionId, String resultCode) throws JsonProcessingException {
        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String timerId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                currentStepPartitionRetryAttempt.get(partitionId), partitionId);

        ForceResultSignalData signal = new ForceResultSignalData();
        signal.setActivityId(timerId);
        signal.setResultCode(resultCode);
        return recordSignalEvent(signal);
    }

    public List<HistoryEvent> recordPartitionMetadataMarkers(Instant eventTime, String stepName,
                                                             PartitionIdGeneratorResult partitionIdGeneratorResult)
            throws JsonProcessingException {
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(partitionIdGeneratorResult);

        List<String> markerDetailsList = metadata.toMarkerDetailsList();

        List<HistoryEvent> markers = new ArrayList<>();
        for (int i = 0; i < markerDetailsList.size(); i++) {
            markers.add(recordMarker(eventTime, TaskNaming.partitionMetadataMarkerName(stepName, i, markerDetailsList.size()),
                                    markerDetailsList.get(i)));
        }
        return markers;
    }

    public HistoryEvent recordMarker(Instant eventTime, String name, String details) {
        MarkerRecordedEventAttributes attrs = MarkerRecordedEventAttributes.builder()
                                                    .markerName(name).details(details).build();

        HistoryEvent event = HistoryEvent.builder().eventId(eventIds.next()).eventType(EventType.MARKER_RECORDED)
                .eventTimestamp(eventTime).markerRecordedEventAttributes(attrs).build();
        events.add(event);
        return event;
    }

    private HistoryEvent buildActivityScheduledEvent(Instant eventTime, String partitionId) {
        Map<String, String> inputClone = new HashMap<>(currentStepInput);
        if (partitionId != null) {
            inputClone.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
            inputClone.put(StepAttributes.PARTITION_COUNT, Long.toString(currentStepPartitionRetryAttempt.size()));
        }

        Long retryAttempt = currentStepPartitionRetryAttempt.get(partitionId);

        if (retryAttempt != 0) {
            inputClone.put(StepAttributes.RETRY_ATTEMPT, Long.toString(retryAttempt));
        }

        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String activityId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                                                        retryAttempt, partitionId);

        ActivityType type = ActivityType.builder().name(activityName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();

        String inputStr = StepAttributes.encode(inputClone);
        ActivityTaskScheduledEventAttributes attrs = ActivityTaskScheduledEventAttributes.builder().input(inputStr)
                                                           .activityType(type).activityId(activityId)
                                                           .taskList(TaskList.builder().name(workflow.taskList()).build())
                                                           .control(partitionId).build();

        return HistoryEvent.builder().eventId(eventIds.next()).eventType(EventType.ACTIVITY_TASK_SCHEDULED)
                .eventTimestamp(eventTime).activityTaskScheduledEventAttributes(attrs).build();
    }

    private HistoryEvent buildActivityCompletedEvent(Instant eventTime, Map<String, String> output, HistoryEvent scheduledActivity) {
        String outputStr = StepAttributes.encode(output);

        ActivityTaskCompletedEventAttributes attrs = ActivityTaskCompletedEventAttributes.builder().result(outputStr)
                .scheduledEventId(scheduledActivity.eventId()).build();

        return HistoryEvent.builder().eventId(eventIds.next()).eventType(EventType.ACTIVITY_TASK_COMPLETED)
                .eventTimestamp(eventTime).activityTaskCompletedEventAttributes(attrs).build();
    }

    private HistoryEvent buildActivityFailedEvent(Instant eventTime, StepResult result, HistoryEvent scheduledActivity) {
        String details = null;
        if (result.getCause() != null) {
            StringWriter sw = new StringWriter();
            result.getCause().printStackTrace(new PrintWriter(sw));
            details = sw.toString();
            if (details.length() > 32768) {
                details = details.substring(0, 32768);
            }
        }

        ActivityTaskFailedEventAttributes attrs = ActivityTaskFailedEventAttributes.builder()
                .reason(result.getMessage()).details(details)
                .scheduledEventId(scheduledActivity.eventId()).build();

        return HistoryEvent.builder().eventId(eventIds.next()).eventType(EventType.ACTIVITY_TASK_FAILED)
                .eventTimestamp(eventTime).activityTaskFailedEventAttributes(attrs).build();
    }

    private HistoryEvent buildScheduleActivityTaskFailedEvent(Instant eventTime, String partitionId) {
        Long retryAttempt = currentStepPartitionRetryAttempt.get(partitionId);
        String activityName = TaskNaming.activityName(TaskNaming.workflowName(workflow), currentStep);
        String activityId = TaskNaming.createActivityId(TaskNaming.stepNameFromActivityName(activityName),
                retryAttempt, partitionId);

        ActivityType type = ActivityType.builder().name(activityName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();

        ScheduleActivityTaskFailedEventAttributes attrs = ScheduleActivityTaskFailedEventAttributes.builder()
                .activityId(activityId).decisionTaskCompletedEventId(eventIds.next()).activityType(type)
                .cause(ScheduleActivityTaskFailedCause.ACTIVITY_CREATION_RATE_EXCEEDED).build();

        return HistoryEvent.builder().eventId(eventIds.next()).eventType(EventType.SCHEDULE_ACTIVITY_TASK_FAILED)
                .eventTimestamp(eventTime).scheduleActivityTaskFailedEventAttributes(attrs).build();
    }

    private HistoryEvent buildTimerStartedEvent(String timerId, Duration retryDelay) {
        TimerStartedEventAttributes attrs = TimerStartedEventAttributes.builder().timerId(timerId)
                .startToFireTimeout(Long.toString(retryDelay.getSeconds())).build();
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(Instant.now())
                .eventType(EventType.TIMER_STARTED).timerStartedEventAttributes(attrs).build();
    }

    private HistoryEvent buildTimerClosedEvent(String timerId, boolean cancelledTimer) {
        HistoryEvent timerStartEvent = currentStepOpenTimers.get(timerId);

        HistoryEvent.Builder timerEvent = HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(Instant.now());

        if(cancelledTimer) {
            TimerCanceledEventAttributes attrs = TimerCanceledEventAttributes.builder()
                    .timerId(timerStartEvent.timerStartedEventAttributes().timerId())
                    .startedEventId(timerStartEvent.eventId())
                    .build();

            timerEvent.eventType(EventType.TIMER_CANCELED);
            timerEvent.timerCanceledEventAttributes(attrs);
        } else {
            TimerFiredEventAttributes attrs = TimerFiredEventAttributes.builder()
                    .timerId(timerStartEvent.timerStartedEventAttributes().timerId())
                    .startedEventId(timerStartEvent.eventId())
                    .build();

            timerEvent.eventType(EventType.TIMER_FIRED);
            timerEvent.timerFiredEventAttributes(attrs);
        }

        return timerEvent.build();
    }

    private HistoryEvent buildWorkflowCancelRequestedEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_CANCEL_REQUESTED)
                .workflowExecutionCancelRequestedEventAttributes(WorkflowExecutionCancelRequestedEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildWorkflowCanceledEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_CANCELED)
                .workflowExecutionCanceledEventAttributes(WorkflowExecutionCanceledEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildWorkflowCompletedEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_COMPLETED)
                .workflowExecutionCompletedEventAttributes(WorkflowExecutionCompletedEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildWorkflowFailedEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_FAILED)
                .workflowExecutionFailedEventAttributes(WorkflowExecutionFailedEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildWorkflowTerminatedEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_TERMINATED)
                .workflowExecutionTerminatedEventAttributes(WorkflowExecutionTerminatedEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildWorkflowTimedOutEvent(Instant eventTime) {
        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_TIMED_OUT)
                .workflowExecutionTimedOutEventAttributes(WorkflowExecutionTimedOutEventAttributes.builder().build())
                .build();
    }

    private HistoryEvent buildSignalEvent(Instant eventTime, BaseSignalData signalData) throws JsonProcessingException {
        return buildSignalEvent(eventTime, signalData.getSignalType().getFriendlyName(), SignalUtils.encodeSignal(signalData));
    }

    private HistoryEvent buildSignalEvent(Instant eventTime, String rawSignalType, String rawSignalData) {
        WorkflowExecutionSignaledEventAttributes attrs = WorkflowExecutionSignaledEventAttributes.builder()
                .signalName(rawSignalType).input(rawSignalData).build();

        return HistoryEvent.builder().eventId(eventIds.next()).eventTimestamp(eventTime)
                .eventType(EventType.WORKFLOW_EXECUTION_SIGNALED).workflowExecutionSignaledEventAttributes(attrs).build();
    }
}
