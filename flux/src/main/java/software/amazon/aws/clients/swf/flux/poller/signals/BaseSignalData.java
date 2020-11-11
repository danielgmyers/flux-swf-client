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

package software.amazon.aws.clients.swf.flux.poller.signals;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Contains data required for all events.
 *
 * We ignore unknown properties because it's useful for clients to be able to put additional properties in the signal for
 * e.g. audit trail purposes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseSignalData {

    @JsonIgnore
    private Long signalEventId;

    @JsonIgnore
    private OffsetDateTime signalEventTime;

    private String activityId;

    public Long getSignalEventId() {
        return signalEventId;
    }

    /**
     * This is only for internal use by the WorkflowState code, you don't need to set it in tests if you're just building events.
     */
    public void setSignalEventId(Long signalEventId) {
        this.signalEventId = signalEventId;
    }

    public OffsetDateTime getSignalEventTime() {
        return signalEventTime;
    }

    /**
     * This is only for internal use by the WorkflowState code, you don't need to set it in tests if you're just building events.
     */
    public void setSignalEventTime(OffsetDateTime signalEventTime) {
        this.signalEventTime = signalEventTime;
    }

    public String getActivityId() {
        return activityId;
    }

    /**
     * The code expects this activityId to match the id of the *next* scheduled attempt of the step,
     * which also happens to match the id of the retry timer.
     */
    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    @JsonIgnore
    public abstract SignalType getSignalType();

    @JsonIgnore
    public boolean isValidSignalInput() {
        return (activityId != null && !activityId.isEmpty());
    }
}
