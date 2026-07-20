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
 * Represents an ASL Choice state, which adds branching logic to a state machine.
 * The Choice state evaluates its rules in order and transitions to the first matching rule's Next state.
 * A Choice state MUST NOT be an End state and does not use Next at the state level.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChoiceState extends AslState {

    @JsonProperty("Choices")
    private List<ChoiceRule> choices;

    @JsonProperty("Default")
    private String defaultState;

    public ChoiceState() {
        super("Choice");
    }

    public List<ChoiceRule> getChoices() {
        return choices;
    }

    public void setChoices(List<ChoiceRule> choices) {
        this.choices = choices;
    }

    public String getDefaultState() {
        return defaultState;
    }

    public void setDefaultState(String defaultState) {
        this.defaultState = defaultState;
    }
}
