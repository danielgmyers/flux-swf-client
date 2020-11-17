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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import software.amazon.aws.clients.swf.flux.poller.signals.BaseSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.ForceResultSignalData;
import software.amazon.aws.clients.swf.flux.poller.signals.SignalType;
import software.amazon.aws.clients.swf.flux.poller.signals.SignalUtils;
import software.amazon.aws.clients.swf.flux.poller.timers.TimerData;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;

// package-private, we don't want anyone except DecisionTaskPoller using this
final class WorkflowState {

    // package-private for use in tests
    static final String FORCED_RESULT_MESSAGE = "Activity forcibly completed due to "
                                                + SignalType.FORCE_RESULT.getFriendlyName() + " signal.";

    // package-private for use in PartitionState and tests
    static final Set<EventType> ACTIVITY_START_EVENTS = new HashSet<>();
    static final Set<EventType> ACTIVITY_CLOSED_EVENTS = new HashSet<>();
    static final Set<EventType> ACTIVITY_SCHEDULING_FAILURE_EVENTS = new HashSet<>();
    private static final Set<EventType> TIMER_START_EVENTS = new HashSet<>();
    private static final Set<EventType> TIMER_CLOSED_EVENTS = new HashSet<>();
    private static final Set<EventType> SIGNAL_EVENTS = new HashSet<>();
    private static final Set<EventType> WORKFLOW_CANCEL_REQUESTED_EVENTS = new HashSet<>();
    private static final Set<EventType> WORKFLOW_END_EVENTS = new HashSet<>();

    static {
        // If you add an entry here, you must also handle it in getStepData and getActivityName.
        ACTIVITY_START_EVENTS.add(EventType.ACTIVITY_TASK_SCHEDULED);
        ACTIVITY_START_EVENTS.add(EventType.WORKFLOW_EXECUTION_STARTED);

        // If you add an entry here, you must also handle it in getStepData, getStepResultAction and getScheduledEventId.
        ACTIVITY_CLOSED_EVENTS.add(EventType.ACTIVITY_TASK_COMPLETED);
        ACTIVITY_CLOSED_EVENTS.add(EventType.ACTIVITY_TASK_TIMED_OUT);
        ACTIVITY_CLOSED_EVENTS.add(EventType.ACTIVITY_TASK_FAILED);
        ACTIVITY_CLOSED_EVENTS.add(EventType.ACTIVITY_TASK_CANCELED);

        ACTIVITY_SCHEDULING_FAILURE_EVENTS.add(EventType.SCHEDULE_ACTIVITY_TASK_FAILED);

        TIMER_START_EVENTS.add(EventType.TIMER_STARTED);

        // If you add an entry here, you must also handle it in getClosedTimerId and getTimerStartedEventId
        TIMER_CLOSED_EVENTS.add(EventType.TIMER_FIRED);
        TIMER_CLOSED_EVENTS.add(EventType.TIMER_CANCELED);

        SIGNAL_EVENTS.add(EventType.WORKFLOW_EXECUTION_SIGNALED);

        WORKFLOW_CANCEL_REQUESTED_EVENTS.add(EventType.WORKFLOW_EXECUTION_CANCEL_REQUESTED);

        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_CANCELED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_COMPLETED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_FAILED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_TERMINATED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_TIMED_OUT);
    }

    private String workflowId;
    private String workflowRunId;
    private OffsetDateTime workflowStartDate;
    private Map<String, String> workflowInput;
    private String currentActivityName;
    private String currentStepResultCode;
    private OffsetDateTime currentStepFirstScheduledTime;
    private OffsetDateTime currentStepCompletionTime;
    private String currentStepLastActivityCompletionMessage;
    private Long currentStepMaxRetryCount;
    private Map<String, Map<String, List<PartitionState>>> stepPartitions;
    private Map<String, TimerData> openTimers;
    private Map<String, Long> closedTimers;
    private Map<String, BaseSignalData> signalsByActivityId;
    private OffsetDateTime workflowCancelRequestDate;
    private boolean workflowExecutionClosed;

    public String getWorkflowId() {
        return workflowId;
    }

    public String getWorkflowRunId() {
        return workflowRunId;
    }

    public OffsetDateTime getWorkflowStartDate() {
        return workflowStartDate;
    }

    public Map<String, String> getWorkflowInput() {
        return workflowInput;
    }

    public String getCurrentActivityName() {
        return currentActivityName;
    }

    public String getCurrentStepResultCode() {
        return currentStepResultCode;
    }

    public OffsetDateTime getCurrentStepFirstScheduledTime() {
        return currentStepFirstScheduledTime;
    }

    public OffsetDateTime getCurrentStepCompletionTime() {
        return currentStepCompletionTime;
    }

    public String getCurrentStepLastActivityCompletionMessage() {
        return currentStepLastActivityCompletionMessage;
    }

    public Long getCurrentStepMaxRetryCount() {
        return currentStepMaxRetryCount;
    }

    public Map<String, Map<String, List<PartitionState>>> getStepPartitions() {
        return Collections.unmodifiableMap(stepPartitions);
    }

    public Map<String, TimerData> getOpenTimers() {
        return Collections.unmodifiableMap(openTimers);
    }

    public Map<String, Long> getClosedTimers() {
        return Collections.unmodifiableMap(closedTimers);
    }

    /**
     * Stores the latest signal received for the activity (as determined by their relative event ids).
     */
    public Map<String, BaseSignalData> getSignalsByActivityId() {
        return Collections.unmodifiableMap(signalsByActivityId);
    }

    public boolean isWorkflowCancelRequested() {
        return workflowCancelRequestDate != null;
    }

    public OffsetDateTime getWorkflowCancelRequestDate() {
        return workflowCancelRequestDate;
    }

    public boolean isWorkflowExecutionClosed() {
        return workflowExecutionClosed;
    }

    // WorkflowState objects should only be created via the static build() method
    private WorkflowState() {}

    /**
     * Builds a representation of the current state of the workflow.
     *
     * If the workflow has just started, then:
     * - getCurrentStepInput() will return the input provided to the workflow.
     * - getCurrentActivityName(), and getCurrentStepResult() will return null.
     * - getCurrentStepOutput(), getOpenTimers() and getClosedTimers() will return empty collections.
     *
     * Otherwise:
     * - getCurrentActivityName() will return the ActivityType's name of the step which was most recently executed.
     * - getCurrentStepInput() will return the attributes provided as input to the most recently executed step.
     * - getCurrentStepOutput() will return the attributes provided as output from the most recently executed step.
     * - getCurrentStepResult() will return the ResultAction appropriate for the step, as determined by getStepResultAction().
     * - getOpenTimers() will return a map containing any open timers (mapped timerId->timerExpirationTime)
     * - getClosedTimers() will return a set containing the id of all timers that have closed.
     *
     * @param task The DecisionTask which should be used to build the current workflow state
     * @return A WorkflowState object representing the current workflow state.
     */
    public static WorkflowState build(PollForDecisionTaskResponse task) {
        HistoryEvent mostRecentClosedEvent = null;
        HistoryEvent mostRecentStartedEvent = null;

        WorkflowState ws = new WorkflowState();
        ws.openTimers = new HashMap<>();
        ws.closedTimers = new HashMap<>();
        ws.stepPartitions = new HashMap<>();
        ws.signalsByActivityId = new HashMap<>();

        ws.workflowCancelRequestDate = null;
        ws.workflowExecutionClosed = false;

        ws.workflowId = task.workflowExecution().workflowId();
        ws.workflowRunId = task.workflowExecution().runId();

        Map<Long, String> closedTimersByStartedEventId = new HashMap<>();

        Map<Long, HistoryEvent> closedEventsByScheduledEventId = new HashMap<>();

        // the events should be in reverse-chronological order
        for (HistoryEvent event : task.events()) {
            if (ACTIVITY_SCHEDULING_FAILURE_EVENTS.contains(event.eventType())) {
                // This case is a bit weird. Basically, we submitted a ScheduleActivityTask decision,
                // and SWF failed to schedule it as requested (e.g. we may have exceeded their rate limit
                // for scheduling activities).
                // We'll deal with this by treating it as if we know the partition id but have not yet scheduled it.
                // We also have to extract the partition id from the activity id since it's not stored anywhere else.
                String activityName = event.scheduleActivityTaskFailedEventAttributes().activityType().name();
                String stepName = TaskNaming.stepNameFromActivityName(activityName);
                String activityId = event.scheduleActivityTaskFailedEventAttributes().activityId();
                String retryAttemptAndPartitionId = activityId.substring(stepName.length() + 1); // skip the step name and first _
                String partitionId = null;
                if (retryAttemptAndPartitionId.contains("_")) {
                    int underscorePos = retryAttemptAndPartitionId.indexOf("_");
                    partitionId = retryAttemptAndPartitionId.substring(underscorePos + 1);
                }

                // If the step isn't partitioned, we can just ignore this event. It'll get rescheduled properly.
                // Otherwise, we need to save the partition id for later reference.
                if (partitionId != null) {
                    if (!ws.stepPartitions.containsKey(activityName)) {
                        ws.stepPartitions.put(activityName, new HashMap<>());
                    }

                    if (!ws.stepPartitions.get(activityName).containsKey(partitionId)) {
                        // we use a linked list because we're building the list in reverse order, back to front,
                        // so every insert will be at the beginning.
                        ws.stepPartitions.get(activityName).put(partitionId, new LinkedList<>());
                    }
                }
            } else if (ACTIVITY_CLOSED_EVENTS.contains(event.eventType())) {
                if (mostRecentClosedEvent == null) {
                    mostRecentClosedEvent = event;
                }
                closedEventsByScheduledEventId.put(getScheduledEventId(event), event);
            } else if (ACTIVITY_START_EVENTS.contains(event.eventType())) {
                if (mostRecentStartedEvent == null) {
                    mostRecentStartedEvent = event;
                }

                if (EventType.WORKFLOW_EXECUTION_STARTED.equals(event.eventType())) {
                    ws.workflowStartDate = OffsetDateTime.ofInstant(event.eventTimestamp(), ZoneOffset.UTC);
                    ws.workflowInput = getStepData(event);
                } else if (EventType.ACTIVITY_TASK_SCHEDULED.equals(event.eventType())) {
                    String activityName = getActivityName(event);
                    PartitionState partition = PartitionState.build(event, closedEventsByScheduledEventId.get(event.eventId()));

                    if (!ws.stepPartitions.containsKey(activityName)) {
                        ws.stepPartitions.put(activityName, new HashMap<>());
                    }

                    if (!ws.stepPartitions.get(activityName).containsKey(partition.getPartitionId())) {
                        // we use a linked list because we're building the list in reverse order, back to front,
                        // so every insert will be at the beginning.
                        ws.stepPartitions.get(activityName).put(partition.getPartitionId(), new LinkedList<>());
                    }

                    ws.stepPartitions.get(activityName).get(partition.getPartitionId()).add(0, partition);
                }
            } else if (TIMER_START_EVENTS.contains(event.eventType())) {
                // The timer will be in the closedTimersByStartedEventId map if it is already closed.
                if (!closedTimersByStartedEventId.containsKey(event.eventId())) {
                    TimerData timerData = new TimerData(event);
                    ws.openTimers.put(timerData.getTimerId(), timerData);
                }
            } else if (TIMER_CLOSED_EVENTS.contains(event.eventType())) {
                String timerId = getClosedTimerId(event);
                long startEventId = getTimerStartedEventId(event);
                closedTimersByStartedEventId.put(startEventId, timerId);
                // If the timer is in the open timers list, it's because it was reopened.
                // In that case, don't add it to the closed list.
                // If it's in the closed timers list already, then a later version of the timer was already fired;
                // in that case, we don't want to overwrite what's already in the closed list.
                if (!ws.openTimers.containsKey(timerId) && !ws.closedTimers.containsKey(timerId)) {
                    ws.closedTimers.put(timerId, event.eventId());
                }
            } else if (SIGNAL_EVENTS.contains(event.eventType())) {
                SignalType type = SignalType.fromFriendlyName(event.workflowExecutionSignaledEventAttributes().signalName());
                if (type != null) {
                    BaseSignalData signalData = SignalUtils.decodeSignal(event.workflowExecutionSignaledEventAttributes());
                    if (signalData != null) {
                        // since these events are in reverse-chronological order, we can guarantee we only keep the most recent
                        // event of each type by only saving this signal if we don't already have one.
                        if (!ws.signalsByActivityId.containsKey(signalData.getActivityId())) {
                            signalData.setSignalEventId(event.eventId());
                            signalData.setSignalEventTime(OffsetDateTime.ofInstant(event.eventTimestamp(),
                                                                                   ZoneOffset.UTC));
                            ws.signalsByActivityId.put(signalData.getActivityId(), signalData);
                        }
                    }
                }
            } else if (WORKFLOW_CANCEL_REQUESTED_EVENTS.contains(event.eventType())) {
                // if more than one cancellation request was sent, we'll just use the most recent one
                if (ws.workflowCancelRequestDate == null) {
                    ws.workflowCancelRequestDate = OffsetDateTime.ofInstant(event.eventTimestamp(), ZoneOffset.UTC);
                }
            } else if (WORKFLOW_END_EVENTS.contains(event.eventType())) {
                ws.workflowExecutionClosed = true;
            }
        }

        if (mostRecentStartedEvent == null) {
            throw new BadWorkflowStateException("Unable to handle a workflow with no start event");
        } else if (ws.workflowStartDate == null) {
            throw new BadWorkflowStateException("Unable to handle a workflow with no WorkflowExecutionStarted event");
        }

        ws.currentActivityName = getActivityName(mostRecentStartedEvent);

        if (ws.currentActivityName != null) {
            Map<String, List<PartitionState>> currentStepPartitions = ws.stepPartitions.get(ws.currentActivityName);
            if (currentStepPartitions.isEmpty()) {
                throw new BadWorkflowStateException("Found a workflow step with no history");
            }

            ws.currentStepFirstScheduledTime = null;
            ws.currentStepResultCode = StepResult.SUCCEED_RESULT_CODE;
            ws.currentStepCompletionTime = null;
            ws.currentStepLastActivityCompletionMessage = null;
            ws.currentStepMaxRetryCount = 0L;

            boolean hasPartitionNeedingRetry = false;

            for (List<PartitionState> partitionHistory : currentStepPartitions.values()) {
                if (partitionHistory.isEmpty()) {
                    // If we get here, then we tried to schedule this partition but we got a ScheduleActivityFailedEvent.
                    // In this case, we'll need to treat the partition as needing to be retried/rescheduled.
                    hasPartitionNeedingRetry = true;
                    continue;
                }
                PartitionState lastState = partitionHistory.get(partitionHistory.size() - 1);

                if (lastState.getResultCode() != null) {
                    ws.currentStepLastActivityCompletionMessage
                            = lastState.getAttemptOutput().get(StepAttributes.ACTIVITY_COMPLETION_MESSAGE);
                    ws.currentStepCompletionTime = lastState.getAttemptCompletedTime();
                }

                ws.currentStepMaxRetryCount = Math.max(ws.currentStepMaxRetryCount, lastState.getRetryAttempt());

                PartitionState firstState = partitionHistory.get(0);
                if (ws.currentStepFirstScheduledTime == null
                        || ws.currentStepFirstScheduledTime.isAfter(firstState.getAttemptScheduledTime())) {
                    ws.currentStepFirstScheduledTime = firstState.getAttemptScheduledTime();
                }

                String stepName = TaskNaming.stepNameFromActivityName(ws.currentActivityName);
                String signalActivityId = TaskNaming.createActivityId(stepName, lastState.getRetryAttempt() + 1,
                                                    lastState.getPartitionId());

                String effectiveResultCode = lastState.getResultCode();
                BaseSignalData signal = ws.signalsByActivityId.get(signalActivityId);
                if (signal != null && signal.getSignalType() == SignalType.FORCE_RESULT) {
                    effectiveResultCode = ((ForceResultSignalData)signal).getResultCode();
                    if (ws.currentStepCompletionTime == null
                            || ws.currentStepCompletionTime.isBefore(signal.getSignalEventTime())) {
                        ws.currentStepCompletionTime = signal.getSignalEventTime();
                    }
                    if (ws.currentStepLastActivityCompletionMessage == null) {
                        ws.currentStepLastActivityCompletionMessage = FORCED_RESULT_MESSAGE;
                    }
                }

                if (lastState.getPartitionCount() == 0) {
                    ws.currentStepResultCode = effectiveResultCode;
                } else {
                    // For partitioned steps, we need to be more selective about how we generate the current result code.
                    // If any partitions need to retry, we retry those partitions.
                    // If no partitions need to retry, and at least one partition failed, we use that result.
                    // Otherwise, we succeed.
                    if (effectiveResultCode != null
                            && !StepResult.FAIL_RESULT_CODE.equals(ws.currentStepResultCode)) {
                        ws.currentStepResultCode = effectiveResultCode;
                    } else if (effectiveResultCode == null) {
                        // we dont have a resultCode for the partition it means we need to retry it
                        hasPartitionNeedingRetry = true;
                        break;
                    }
                }
            }

            if (hasPartitionNeedingRetry) {
                ws.currentStepResultCode = null;
                ws.currentStepCompletionTime = null;
            }
        }
        return ws;
    }

    private static String getClosedTimerId(HistoryEvent event) {
        switch (event.eventType()) {
            case TIMER_FIRED:
                return event.timerFiredEventAttributes().timerId();
            case TIMER_CANCELED:
                return event.timerCanceledEventAttributes().timerId();
            default:
                // If we get here, then someone added an entry to TIMER_CLOSED_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to determine timer id for event of type " + event.eventTypeAsString());
        }
    }

    private static Long getTimerStartedEventId(HistoryEvent event) {
        switch (event.eventType()) {
            case TIMER_FIRED:
                return event.timerFiredEventAttributes().startedEventId();
            case TIMER_CANCELED:
                return event.timerCanceledEventAttributes().startedEventId();
            default:
                // If we get here, then someone added an entry to TIMER_CLOSED_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to determine started event id for event of type " + event.eventTypeAsString());
        }
    }

    // package-private for testing
    static Map<String, String> getStepData(HistoryEvent event) {
        String data;
        switch (event.eventType()) {
            case WORKFLOW_EXECUTION_STARTED:
                data = event.workflowExecutionStartedEventAttributes().input();
                break;
            case ACTIVITY_TASK_SCHEDULED:
                data = event.activityTaskScheduledEventAttributes().input();
                break;
            case ACTIVITY_TASK_COMPLETED:
                data = event.activityTaskCompletedEventAttributes().result();
                break;
            case ACTIVITY_TASK_TIMED_OUT:
                data = event.activityTaskTimedOutEventAttributes().details();
                break;
            case ACTIVITY_TASK_CANCELED:
                data = event.activityTaskCanceledEventAttributes().details();
                break;
            case ACTIVITY_TASK_FAILED:
                data = null; // retries don't produce any step data.
                break;
            default:
                // If we get here, then someone added an entry to ACTIVITY_START_EVENTS or ACTIVITY_CLOSED_EVENTS
                // but didn't handle it here.
                throw new RuntimeException("Unable to retrieve step data for event of type " + event.eventTypeAsString());
        }

        return StepAttributes.decode(Map.class, data);
    }

    // package-private for testing
    static String getActivityName(HistoryEvent event) {
        switch (event.eventType()) {
            case WORKFLOW_EXECUTION_STARTED:
                // intentionally return null since there isn't a "current" step in this case
                return null;
            case ACTIVITY_TASK_SCHEDULED:
                return event.activityTaskScheduledEventAttributes().activityType().name();
            default:
                // If we get here, then someone added an entry to ACTIVITY_START_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to determine step name for event of type " + event.eventTypeAsString());
        }
    }

    // package-private for use in PartitionState and tests
    static StepResult.ResultAction getStepResultAction(HistoryEvent event) {
        switch (event.eventType()) {
            case ACTIVITY_TASK_COMPLETED:
                return StepResult.ResultAction.COMPLETE;
            case ACTIVITY_TASK_TIMED_OUT:
                return StepResult.ResultAction.RETRY;
            case ACTIVITY_TASK_CANCELED:
                return StepResult.ResultAction.RETRY;
            case ACTIVITY_TASK_FAILED:
                return StepResult.ResultAction.RETRY;
            default:
                // If we get here, then someone added an entry to ACTIVITY_CLOSED_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to retrieve step output result for event of type " + event.eventTypeAsString());
        }
    }

    // package-private for use in PartitionState and tests
    static Long getScheduledEventId(HistoryEvent event) {
        switch (event.eventType()) {
            case ACTIVITY_TASK_COMPLETED:
                return event.activityTaskCompletedEventAttributes().scheduledEventId();
            case ACTIVITY_TASK_TIMED_OUT:
                return event.activityTaskTimedOutEventAttributes().scheduledEventId();
            case ACTIVITY_TASK_CANCELED:
                return event.activityTaskCanceledEventAttributes().scheduledEventId();
            case ACTIVITY_TASK_FAILED:
                return event.activityTaskFailedEventAttributes().scheduledEventId();
            default:
                // If we get here, then someone added an entry to ACTIVITY_CLOSED_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to retrieve scheduled event id for event of type " + event.eventTypeAsString());
        }
    }
}
