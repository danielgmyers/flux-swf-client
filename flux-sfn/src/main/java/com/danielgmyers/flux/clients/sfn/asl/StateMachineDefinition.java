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

package com.danielgmyers.flux.clients.sfn.asl;

import java.util.Map;

import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the top-level Amazon States Language state machine definition.
 * This is the root object that serializes to a complete ASL JSON document.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StateMachineDefinition {

    @JsonProperty("Comment")
    private String comment;

    @JsonProperty("StartAt")
    private String startAt;

    @JsonProperty("States")
    private Map<String, AslState> states;

    @JsonProperty("Version")
    private String version;

    @JsonProperty("TimeoutSeconds")
    private Integer timeoutSeconds;

    @JsonProperty("QueryLanguage")
    private String queryLanguage;

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getStartAt() {
        return startAt;
    }

    public void setStartAt(String startAt) {
        this.startAt = startAt;
    }

    public Map<String, AslState> getStates() {
        return states;
    }

    public void setStates(Map<String, AslState> states) {
        this.states = states;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(Integer timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public void setQueryLanguage(String queryLanguage) {
        this.queryLanguage = queryLanguage;
    }
}
