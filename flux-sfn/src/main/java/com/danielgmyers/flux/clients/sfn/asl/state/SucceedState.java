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

/**
 * Represents an ASL Succeed state, which terminates the state machine successfully.
 * It is a terminal state and has no Next field.
 * Useful as a target for Choice-State branches that don't do anything except terminate.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SucceedState extends AslState {

    public SucceedState() {
        super("Succeed");
    }
}
