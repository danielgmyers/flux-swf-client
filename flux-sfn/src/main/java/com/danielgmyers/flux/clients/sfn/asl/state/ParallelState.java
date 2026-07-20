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
 * Represents an ASL Parallel state, which executes multiple branches concurrently.
 * The result is an array containing one element for each branch, in the same order
 * as the Branches array.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ParallelState extends AslState {

    @JsonProperty("Branches")
    private List<Branch> branches;

    @JsonProperty("Parameters")
    private Map<String, Object> parameters;

    @JsonProperty("ResultSelector")
    private Map<String, Object> resultSelector;

    @JsonProperty("ResultPath")
    private String resultPath;

    @JsonProperty("Retry")
    private List<Retrier> retry;

    @JsonProperty("Catch")
    private List<Catcher> catchers;

    public ParallelState() {
        super("Parallel");
    }

    public List<Branch> getBranches() {
        return branches;
    }

    public void setBranches(List<Branch> branches) {
        this.branches = branches;
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

    /**
     * Represents a branch within a Parallel state. Each branch is a sub-state-machine
     * with its own StartAt and States fields.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Branch {

        @JsonProperty("StartAt")
        private String startAt;

        @JsonProperty("States")
        private Map<String, AslState> states;

        @JsonProperty("Comment")
        private String comment;

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

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }
}
