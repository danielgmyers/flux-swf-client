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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an ASL Catcher, used to handle errors on Task, Parallel, and Map states.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Catcher {

    @JsonProperty("ErrorEquals")
    private List<String> errorEquals;

    @JsonProperty("Next")
    private String next;

    @JsonProperty("ResultPath")
    private String resultPath;

    public List<String> getErrorEquals() {
        return errorEquals;
    }

    public void setErrorEquals(List<String> errorEquals) {
        this.errorEquals = errorEquals;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public String getResultPath() {
        return resultPath;
    }

    public void setResultPath(String resultPath) {
        this.resultPath = resultPath;
    }
}
