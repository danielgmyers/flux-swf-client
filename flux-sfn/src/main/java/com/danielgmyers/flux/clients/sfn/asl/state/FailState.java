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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an ASL Fail state, which terminates the state machine and marks it as a failure.
 * It is a terminal state and has no Next field.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FailState extends AslState {

    @JsonProperty("Error")
    private String error;

    @JsonProperty("ErrorPath")
    private String errorPath;

    @JsonProperty("Cause")
    private String cause;

    @JsonProperty("CausePath")
    private String causePath;

    public FailState() {
        super("Fail");
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getErrorPath() {
        return errorPath;
    }

    public void setErrorPath(String errorPath) {
        this.errorPath = errorPath;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public String getCausePath() {
        return causePath;
    }

    public void setCausePath(String causePath) {
        this.causePath = causePath;
    }
}
