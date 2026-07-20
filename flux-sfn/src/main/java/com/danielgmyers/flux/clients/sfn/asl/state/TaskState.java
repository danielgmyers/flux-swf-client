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

package com.danielgmyers.flux.clients.sfn.asl.state;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an ASL Task state, which causes the interpreter to execute work
 * identified by the Resource field.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TaskState extends AslState {

    @JsonProperty("Resource")
    private String resource;

    @JsonProperty("Parameters")
    private Map<String, Object> parameters;

    @JsonProperty("ResultSelector")
    private Map<String, Object> resultSelector;

    @JsonProperty("ResultPath")
    private String resultPath;

    @JsonProperty("TimeoutSeconds")
    private Integer timeoutSeconds;

    @JsonProperty("TimeoutSecondsPath")
    private String timeoutSecondsPath;

    @JsonProperty("HeartbeatSeconds")
    private Integer heartbeatSeconds;

    @JsonProperty("HeartbeatSecondsPath")
    private String heartbeatSecondsPath;

    @JsonProperty("Credentials")
    private Map<String, Object> credentials;

    @JsonProperty("Retry")
    private List<Retrier> retry;

    @JsonProperty("Catch")
    private List<Catcher> catchers;

    public TaskState() {
        super("Task");
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public Map<String, Object> getResultSelector() {
        return resultSelector;
    }

    public void setResultSelector(Map<String, Object> resultSelector) {
        this.resultSelector = resultSelector;
    }

    public String getResultPath() {
        return resultPath;
    }

    public void setResultPath(String resultPath) {
        this.resultPath = resultPath;
    }

    public Integer getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(Integer timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getTimeoutSecondsPath() {
        return timeoutSecondsPath;
    }

    public void setTimeoutSecondsPath(String timeoutSecondsPath) {
        this.timeoutSecondsPath = timeoutSecondsPath;
    }

    public Integer getHeartbeatSeconds() {
        return heartbeatSeconds;
    }

    public void setHeartbeatSeconds(Integer heartbeatSeconds) {
        this.heartbeatSeconds = heartbeatSeconds;
    }

    public String getHeartbeatSecondsPath() {
        return heartbeatSecondsPath;
    }

    public void setHeartbeatSecondsPath(String heartbeatSecondsPath) {
        this.heartbeatSecondsPath = heartbeatSecondsPath;
    }

    public Map<String, Object> getCredentials() {
        return credentials;
    }

    public void setCredentials(Map<String, Object> credentials) {
        this.credentials = credentials;
    }

    public List<Retrier> getRetry() {
        return retry;
    }

    public void setRetry(List<Retrier> retry) {
        this.retry = retry;
    }

    public List<Catcher> getCatchers() {
        return catchers;
    }

    public void setCatchers(List<Catcher> catchers) {
        this.catchers = catchers;
    }
}
