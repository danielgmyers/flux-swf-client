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

package com.danielgmyers.flux.wf.graph.testhooks;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepHook;
import com.danielgmyers.flux.step.WorkflowStepHook;

public class TestPostStepHook implements WorkflowStepHook {

    private int postStepHookCallCount = 0;

    @StepHook(hookType = StepHook.HookType.POST)
    public void postStepHook(@Attribute(StepAttributes.RESULT_CODE) String resultCode,
                             @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String completionMessage) {
        postStepHookCallCount += 1;
    }

    public int getPostStepHookCallCount() {
        return postStepHookCallCount;
    }
}
