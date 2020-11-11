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

package software.amazon.aws.clients.swf.flux.poller.timers;

import java.time.Instant;
import java.util.Objects;

import software.amazon.awssdk.services.swf.model.HistoryEvent;

/**
 * Stores data related to open timers.
 */
public class TimerData {

    private Long startTimerEventId;
    private String timerId;
    private Instant endTime;

    /**
     * Constructs a TimerData object given a workflow event containing valid TimerStartedEventAttributes.
     */
    public TimerData(HistoryEvent startEvent) {
        if (startEvent == null || startEvent.timerStartedEventAttributes() == null) {
            throw new IllegalArgumentException("Cannot construct TimerData without valid TimerStartedEventAttributes.");
        }
        this.startTimerEventId = startEvent.eventId();
        this.timerId = startEvent.timerStartedEventAttributes().timerId();
        long startToFireTimeout = Long.parseLong(startEvent.timerStartedEventAttributes().startToFireTimeout());
        this.endTime = startEvent.eventTimestamp().plusSeconds(startToFireTimeout);
    }

    public Long getStartTimerEventId() {
        return startTimerEventId;
    }

    public void setStartTimerEventId(Long startTimerEventId) {
        this.startTimerEventId = startTimerEventId;
    }

    public String getTimerId() {
        return timerId;
    }

    public void setTimerId(String timerId) {
        this.timerId = timerId;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    @Override
    public boolean equals(Object rhs) {
        if (this == rhs) {
            return true;
        }
        if (rhs == null || getClass() != rhs.getClass()) {
            return false;
        }

        TimerData timerData = (TimerData) rhs;

        if (!Objects.equals(startTimerEventId, timerData.startTimerEventId)) {
            return false;
        }
        if (!Objects.equals(timerId, timerData.timerId)) {
            return false;
        }
        return Objects.equals(endTime, timerData.endTime);
    }

    @Override
    public int hashCode() {
        int result = startTimerEventId != null ? startTimerEventId.hashCode() : 0;
        result = 31 * result + (timerId != null ? timerId.hashCode() : 0);
        result = 31 * result + (endTime != null ? endTime.hashCode() : 0);
        return result;
    }
}
