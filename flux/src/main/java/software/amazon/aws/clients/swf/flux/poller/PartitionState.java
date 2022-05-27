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

package software.amazon.aws.clients.swf.flux.poller;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.awssdk.services.swf.model.EventType;
import software.amazon.awssdk.services.swf.model.HistoryEvent;

/**
 * Stores the step state relevant to a specific partition of a specific workflow step.
 *
 * This is used for non-partitioned steps too, they're treated as single-partition steps with a null partition id.
 */
public final class PartitionState {

    private String partitionId;
    private String activityId;
    private Instant attemptScheduledTime;
    private Instant attemptCompletedTime;
    private Map<String, String> attemptInput;
    private Map<String, String> attemptOutput;
    private StepResult.ResultAction attemptResult;

    /**
     * Builds a PartitionState based on a given scheduledEvent and closedEvent.
     * The closedEvent may be null if the scheduled activity has not closed yet.
     */
    public static PartitionState build(HistoryEvent scheduledEvent, HistoryEvent closedEvent) {
        if (scheduledEvent.eventType() != EventType.ACTIVITY_TASK_SCHEDULED) {
            throw new RuntimeException("The scheduledEvent's type must be " + EventType.ACTIVITY_TASK_SCHEDULED);
        }
        if (closedEvent != null && !WorkflowState.ACTIVITY_CLOSED_EVENTS.contains(closedEvent.eventType())) {
            throw new RuntimeException("The closedEvent's type must be one of: "
                                       + WorkflowState.ACTIVITY_CLOSED_EVENTS.stream().map(EventType::toString)
                                                                             .collect(Collectors.joining(",")));
        }

        if (closedEvent != null) {
            Long scheduledEventIdForClosedEvent = WorkflowState.getScheduledEventId(closedEvent);
            if (!scheduledEvent.eventId().equals(scheduledEventIdForClosedEvent)) {
                throw new RuntimeException("The closedEvent's ScheduledEventId should correspond to the provided scheduledEvent.");
            }
        }

        PartitionState state = new PartitionState();
        state.partitionId = scheduledEvent.activityTaskScheduledEventAttributes().control();
        state.activityId = scheduledEvent.activityTaskScheduledEventAttributes().activityId();
        state.attemptScheduledTime = scheduledEvent.eventTimestamp();
        state.attemptInput = WorkflowState.getStepData(scheduledEvent);

        if (closedEvent != null) {
            state.attemptResult = WorkflowState.getStepResultAction(closedEvent);
            state.attemptOutput = WorkflowState.getStepData(closedEvent);
            state.attemptCompletedTime = closedEvent.eventTimestamp();
        } else {
            state.attemptResult = null;
            state.attemptOutput = Collections.emptyMap();
            state.attemptCompletedTime = null;
        }

        return state;
    }

    private PartitionState() { }

    public String getActivityId() {
        return activityId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public long getPartitionCount() {
        Long count = StepAttributes.decode(Long.class, attemptInput.get(StepAttributes.PARTITION_COUNT));
        return (count == null) ? 0L : count;
    }

    public Instant getAttemptScheduledTime() {
        return attemptScheduledTime;
    }

    public Instant getAttemptCompletedTime() {
        return attemptCompletedTime;
    }

    public long getRetryAttempt() {
        Long attempt = StepAttributes.decode(Long.class, attemptInput.get(StepAttributes.RETRY_ATTEMPT));
        return (attempt == null) ? 0L : attempt;
    }

    public Map<String, String> getAttemptInput() {
        return Collections.unmodifiableMap(attemptInput);
    }

    public Map<String, String> getAttemptOutput() {
        return Collections.unmodifiableMap(attemptOutput);
    }

    public StepResult.ResultAction getAttemptResult() {
        return attemptResult;
    }

    public String getResultCode() {
        return attemptOutput.get(StepAttributes.RESULT_CODE);
    }
}
