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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base class for all Amazon States Language state types.
 * Uses Jackson polymorphic serialization via the "Type" field.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Type", include = JsonTypeInfo.As.EXISTING_PROPERTY, visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TaskState.class, name = "Task"),
    @JsonSubTypes.Type(value = PassState.class, name = "Pass"),
    @JsonSubTypes.Type(value = ChoiceState.class, name = "Choice"),
    @JsonSubTypes.Type(value = WaitState.class, name = "Wait"),
    @JsonSubTypes.Type(value = SucceedState.class, name = "Succeed"),
    @JsonSubTypes.Type(value = FailState.class, name = "Fail"),
    @JsonSubTypes.Type(value = ParallelState.class, name = "Parallel"),
    @JsonSubTypes.Type(value = MapState.class, name = "Map")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AslState {

    @JsonProperty("Type")
    private final String type;

    @JsonProperty("Comment")
    private String comment;

    @JsonProperty("Next")
    private String next;

    @JsonProperty("End")
    private Boolean end;

    @JsonProperty("InputPath")
    private String inputPath;

    @JsonProperty("OutputPath")
    private String outputPath;

    protected AslState(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public Boolean getEnd() {
        return end;
    }

    public void setEnd(Boolean end) {
        this.end = end;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
}
