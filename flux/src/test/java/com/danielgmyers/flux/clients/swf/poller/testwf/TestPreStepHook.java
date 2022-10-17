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

package com.danielgmyers.flux.clients.swf.poller.testwf;

import java.util.Date;

import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.StepHook;
import com.danielgmyers.flux.clients.swf.step.WorkflowStepHook;

public class TestPreStepHook implements WorkflowStepHook {

    private int preStepHookCallCount = 0;

    @StepHook(hookType = StepHook.HookType.PRE)
    public void preStepHook(@Attribute(StepAttributes.ACTIVITY_NAME) String activityName,
                            @Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityStartTime,
                            @Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                            @Attribute(StepAttributes.WORKFLOW_START_TIME) String workflowStartTime) {
        preStepHookCallCount += 1;
    }

    public int getPreStepHookCallCount() {
        return preStepHookCallCount;
    }
}
