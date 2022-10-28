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

package com.danielgmyers.flux.signals;

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private Instant signalEventTime;

    @JsonIgnore
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    public Instant getSignalEventTime() {
        return signalEventTime;
    }

    /**
     * This is only for internal use by the WorkflowState code, you don't need to set it in tests if you're just building events.
     */
    public void setSignalEventTime(Instant signalEventTime) {
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

    /**
     * Decodes the data contained in the specified signalData into a subclass of BaseSignalData based on the signal name.
     * If the data is invalid, or not recognized, or missing, returns null.
     */
    public static BaseSignalData fromJson(String signalName, String signalData) {
        SignalType signalType = SignalType.fromFriendlyName(signalName);
        if (signalType == null) {
            return null;
        }
        try {
            BaseSignalData data = MAPPER.readValue(signalData, signalType.getSignalDataType());
            if (!data.isValidSignalInput()) {
                return null;
            }
            return data;
        } catch (IOException e) {
            // if we get this exception, the signal data was malformed, and we'll ignore the signal.
            return null;
        }
    }

    /**
     * Returns a JSON-encoded string representing this signal's data.
     */
    @JsonIgnore
    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }

    @JsonIgnore
    public abstract SignalType getSignalType();

    @JsonIgnore
    public boolean isValidSignalInput() {
        return (activityId != null && !activityId.isEmpty());
    }
}
