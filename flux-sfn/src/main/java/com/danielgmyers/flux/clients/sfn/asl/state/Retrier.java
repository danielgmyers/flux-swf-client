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
 * Represents an ASL Retrier, used to configure retry behavior on Task, Parallel, and Map states.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Retrier {

    @JsonProperty("ErrorEquals")
    private List<String> errorEquals;

    @JsonProperty("IntervalSeconds")
    private Integer intervalSeconds;

    @JsonProperty("MaxAttempts")
    private Integer maxAttempts;

    @JsonProperty("BackoffRate")
    private Double backoffRate;

    @JsonProperty("MaxDelaySeconds")
    private Integer maxDelaySeconds;

    @JsonProperty("JitterStrategy")
    private String jitterStrategy;

    public List<String> getErrorEquals() {
        return errorEquals;
    }

    public void setErrorEquals(List<String> errorEquals) {
        this.errorEquals = errorEquals;
    }

    public Integer getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(Integer intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Double getBackoffRate() {
        return backoffRate;
    }

    public void setBackoffRate(Double backoffRate) {
        this.backoffRate = backoffRate;
    }

    public Integer getMaxDelaySeconds() {
        return maxDelaySeconds;
    }

    public void setMaxDelaySeconds(Integer maxDelaySeconds) {
        this.maxDelaySeconds = maxDelaySeconds;
    }

    public String getJitterStrategy() {
        return jitterStrategy;
    }

    public void setJitterStrategy(String jitterStrategy) {
        this.jitterStrategy = jitterStrategy;
    }
}
