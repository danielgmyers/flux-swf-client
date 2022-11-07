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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.clients.swf.poller.timers.TimerData;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.signals.BaseSignalData;
import com.danielgmyers.flux.signals.ForceResultSignalData;
import com.danielgmyers.flux.signals.SignalType;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionSignaledEventAttributes;

// package-private, we don't want anyone except DecisionTaskPoller using this
final class WorkflowState {

    private static final Logger log = LoggerFactory.getLogger(WorkflowState.class);

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
    private static final Set<EventType> MARKER_EVENTS = new HashSet<>();
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

        MARKER_EVENTS.add(EventType.MARKER_RECORDED);

        WORKFLOW_CANCEL_REQUESTED_EVENTS.add(EventType.WORKFLOW_EXECUTION_CANCEL_REQUESTED);

        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_CANCELED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_COMPLETED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_FAILED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_TERMINATED);
        WORKFLOW_END_EVENTS.add(EventType.WORKFLOW_EXECUTION_TIMED_OUT);
    }

    private String workflowId;
    private String workflowRunId;
    private Instant workflowStartDate;
    private Map<String, String> workflowInput;
    private String currentActivityName;
    private String currentStepResultCode;
    private Instant currentStepFirstScheduledTime;
    private Instant currentStepCompletionTime;
    private String currentStepLastActivityCompletionMessage;
    private Long currentStepMaxRetryCount;
    private Map<String, List<String>> rawPartitionMetadata;
    private Map<String, Instant> stepInitialAttemptTimes;
    private Map<String, Map<String, PartitionState>> latestPartitionStates;
    private Map<String, TimerData> openTimers;
    private Map<String, Long> closedTimers;
    private Map<String, BaseSignalData> signalsByActivityId;
    private Instant workflowCancelRequestDate;
    private boolean workflowExecutionClosed;

    public String getWorkflowId() {
        return workflowId;
    }

    public String getWorkflowRunId() {
        return workflowRunId;
    }

    public Instant getWorkflowStartDate() {
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

    public Instant getCurrentStepFirstScheduledTime() {
        return currentStepFirstScheduledTime;
    }

    public Instant getCurrentStepCompletionTime() {
        return currentStepCompletionTime;
    }

    public String getCurrentStepLastActivityCompletionMessage() {
        return currentStepLastActivityCompletionMessage;
    }

    public Long getCurrentStepMaxRetryCount() {
        return currentStepMaxRetryCount;
    }

    public PartitionMetadata getPartitionMetadata(String stepName) {
        if (rawPartitionMetadata.containsKey(stepName)) {
            try {
                return PartitionMetadata.fromMarkerDetailsList(rawPartitionMetadata.get(stepName));
            } catch (JsonProcessingException e) {
                // If we couldn't parse it, then for the purposes of representing workflow state we should behave
                // as if the metadata doesn't exist at all.
                log.warn("Unable to parse partition metadata from marker data for step {}", stepName, e);
            }
        }
        return null;
    }

    public Map<String, PartitionState> getLatestPartitionStates(String activityName) {
        if (!latestPartitionStates.containsKey(activityName)) {
            return Collections.emptyMap();
        }
        return latestPartitionStates.get(activityName);
    }

    public Instant getStepInitialAttemptTime(String activityName) {
        return stepInitialAttemptTimes.get(activityName);
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

    public Instant getWorkflowCancelRequestDate() {
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
        ws.rawPartitionMetadata = new HashMap<>();
        ws.openTimers = new HashMap<>();
        ws.closedTimers = new HashMap<>();
        ws.signalsByActivityId = new HashMap<>();

        ws.stepInitialAttemptTimes = new HashMap<>();
        ws.latestPartitionStates = new HashMap<>();

        ws.workflowCancelRequestDate = null;
        ws.workflowExecutionClosed = false;

        ws.workflowId = task.workflowExecution().workflowId();
        ws.workflowRunId = task.workflowExecution().runId();

        Map<Long, String> closedTimersByStartedEventId = new HashMap<>();

        Map<Long, HistoryEvent> closedEventsByScheduledEventId = new HashMap<>();

        Map<String, Set<Long>> partitionMarkerSubsetsFound = new HashMap<>();
        Map<String, List<String>> partitionMarkerSubsets = new HashMap<>();

        // the events should be in reverse-chronological order
        for (HistoryEvent event : task.events()) {
            if (ACTIVITY_SCHEDULING_FAILURE_EVENTS.contains(event.eventType())) {
                // This case is a bit weird. Basically, we submitted a ScheduleActivityTask decision,
                // and SWF failed to schedule it as requested (e.g. we may have exceeded their rate limit
                // for scheduling activities).

                // For the most part we don't care, but eventually we'll want to track that this has happened
                // so that the decider can do proper exponential backoff if desired.
                // See https://github.com/danielgmyers/flux-swf-client/issues/32 for details.
            } else if (MARKER_EVENTS.contains(event.eventType())) {
                // There may be markers we don't care about in the history, such as the "unknown result code" marker that Flux adds,
                // or markers added by other tools.
                String markerName = event.markerRecordedEventAttributes().markerName();
                if (TaskNaming.isPartitionMetadataMarker(markerName)) {
                    String stepName = TaskNaming.extractPartitionMetadataMarkerStepName(markerName);
                    Long subsetId = TaskNaming.extractPartitionMetadataMarkerSubsetId(markerName);

                    partitionMarkerSubsetsFound.putIfAbsent(stepName, new HashSet<>());
                    partitionMarkerSubsetsFound.get(stepName).add(subsetId);

                    partitionMarkerSubsets.putIfAbsent(stepName, new LinkedList<>());
                    partitionMarkerSubsets.get(stepName).add(getMarkerData(event));

                    // If we've found all of the subsets, we can record the aggregate marker data.
                    // We can then clear out the temporary places we were storing it in since we don't need it anymore.
                    Long markerCount = TaskNaming.extractPartitionMetadataMarkerCount(markerName);
                    if (partitionMarkerSubsetsFound.get(stepName).size() == markerCount) {
                        // It's possible a marker was added more than once for some reason,
                        // so only insert if we don't already have marker data for this step.
                        ws.rawPartitionMetadata.putIfAbsent(stepName, partitionMarkerSubsets.get(stepName));

                        partitionMarkerSubsetsFound.remove(stepName);
                        partitionMarkerSubsets.remove(stepName);
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
                    ws.workflowStartDate = event.eventTimestamp();
                    ws.workflowInput = getStepData(event);
                } else if (EventType.ACTIVITY_TASK_SCHEDULED.equals(event.eventType())) {
                    String activityName = getActivityName(event);
                    String partitionId = event.activityTaskScheduledEventAttributes().control();

                    // Since we're iterating over these in reverse order, we can just always overwrite whatever time is here.
                    ws.stepInitialAttemptTimes.put(activityName, event.eventTimestamp());

                    // if we already have newer state for this partition, don't bother creating an older PartitionState.
                    if (!ws.latestPartitionStates.containsKey(activityName)) {
                        ws.latestPartitionStates.put(activityName, new HashMap<>());
                    }
                    if (!ws.latestPartitionStates.get(activityName).containsKey(partitionId)) {
                        PartitionState partition = PartitionState.build(event, closedEventsByScheduledEventId.get(event.eventId()));
                        ws.latestPartitionStates.get(activityName).put(partitionId, partition);
                    }
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
                    WorkflowExecutionSignaledEventAttributes signal = event.workflowExecutionSignaledEventAttributes();
                    BaseSignalData signalData = BaseSignalData.fromJson(signal.signalName(), signal.input());
                    if (signalData != null) {
                        // since these events are in reverse-chronological order, we can guarantee we only keep the most recent
                        // event of each type by only saving this signal if we don't already have one.
                        if (!ws.signalsByActivityId.containsKey(signalData.getActivityId())) {
                            signalData.setSignalEventId(event.eventId());
                            signalData.setSignalEventTime(event.eventTimestamp());
                            ws.signalsByActivityId.put(signalData.getActivityId(), signalData);
                        }
                    }
                }
            } else if (WORKFLOW_CANCEL_REQUESTED_EVENTS.contains(event.eventType())) {
                // if more than one cancellation request was sent, we'll just use the most recent one
                if (ws.workflowCancelRequestDate == null) {
                    ws.workflowCancelRequestDate = event.eventTimestamp();
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
            Map<String, PartitionState> currentStepPartitions = ws.latestPartitionStates.get(ws.currentActivityName);
            if (currentStepPartitions.isEmpty()) {
                throw new BadWorkflowStateException("Found a workflow step with no history");
            }

            ws.currentStepFirstScheduledTime = null;
            ws.currentStepResultCode = StepResult.SUCCEED_RESULT_CODE;
            ws.currentStepCompletionTime = null;
            ws.currentStepLastActivityCompletionMessage = null;
            ws.currentStepMaxRetryCount = 0L;

            String currentStepName = TaskNaming.stepNameFromActivityName(ws.currentActivityName);
            PartitionMetadata metadata = ws.getPartitionMetadata(currentStepName);
            // Ensure we have an entry for every partition ID, even if scheduling failed for some of them.
            // Note that this doesn't help if _all_ of the first attempts failed to schedule, but in that case Flux
            // can just behave as if it hasn't tried at all yet.
            if (metadata != null) {
                for (String partitionId : metadata.getPartitionIds()) {
                    if (!currentStepPartitions.containsKey(partitionId)) {
                        currentStepPartitions.put(partitionId, null);
                    }
                }
            }

            boolean hasPartitionNeedingRetry = false;

            for (Map.Entry<String, PartitionState> e : currentStepPartitions.entrySet()) {
                PartitionState lastState = e.getValue();
                if (lastState == null) {
                    // If we get here, then we tried to schedule this partition but we got a ScheduleActivityFailedEvent.
                    // In this case, we'll need to treat the partition as needing to be retried/rescheduled.
                    hasPartitionNeedingRetry = true;
                    continue;
                }

                if (lastState.getResultCode() != null) {
                    ws.currentStepLastActivityCompletionMessage
                            = lastState.getAttemptOutput().get(StepAttributes.ACTIVITY_COMPLETION_MESSAGE);
                    ws.currentStepCompletionTime = lastState.getAttemptCompletedTime();
                }

                ws.currentStepMaxRetryCount = Math.max(ws.currentStepMaxRetryCount, lastState.getRetryAttempt());

                ws.currentStepFirstScheduledTime = ws.stepInitialAttemptTimes.get(ws.currentActivityName);

                String stepName = TaskNaming.stepNameFromActivityName(ws.currentActivityName);
                String partitionId = e.getKey();
                String signalActivityId = TaskNaming.createActivityId(stepName, lastState.getRetryAttempt() + 1, partitionId);

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
                        // If we don't have a resultCode for the partition, it means we need to retry it.
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

    // package-private for use in tests
    static String getMarkerData(HistoryEvent event) {
        switch (event.eventType()) {
            case MARKER_RECORDED:
                return event.markerRecordedEventAttributes().details();
            default:
                // If we get here, then someone added an entry to MARKER_EVENTS but didn't handle it here.
                throw new RuntimeException("Unable to retrieve marker data for event of type " + event.eventTypeAsString());
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
