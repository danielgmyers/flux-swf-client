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

package com.danielgmyers.flux.wf.graph.teststeps;

import java.time.Instant;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;

public class TestStepExpectsStartTimeAttributesAsInstant implements WorkflowStep {

    @StepApply
    public void apply(@Attribute(StepAttributes.WORKFLOW_START_TIME) Instant workflowStart,
                      @Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Instant activityInitialStart) {
    }
}
