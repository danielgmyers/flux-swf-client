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
 * Represents an ASL Wait state, which delays the state machine for a specified time.
 * Exactly one of Seconds, SecondsPath, Timestamp, or TimestampPath must be specified.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WaitState extends AslState {

    @JsonProperty("Seconds")
    private Integer seconds;

    @JsonProperty("SecondsPath")
    private String secondsPath;

    @JsonProperty("Timestamp")
    private String timestamp;

    @JsonProperty("TimestampPath")
    private String timestampPath;

    public WaitState() {
        super("Wait");
    }

    public Integer getSeconds() {
        return seconds;
    }

    public void setSeconds(Integer seconds) {
        this.seconds = seconds;
    }

    public String getSecondsPath() {
        return secondsPath;
    }

    public void setSecondsPath(String secondsPath) {
        this.secondsPath = secondsPath;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestampPath() {
        return timestampPath;
    }

    public void setTimestampPath(String timestampPath) {
        this.timestampPath = timestampPath;
    }
}
