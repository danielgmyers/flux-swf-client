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

import com.danielgmyers.flux.clients.swf.step.StepHook;
import com.danielgmyers.flux.clients.swf.step.WorkflowStepHook;
import com.danielgmyers.metrics.MetricRecorder;

public class TestHookWithMetrics implements WorkflowStepHook {

    public static final String PRE_HOOK_METRIC_NAME = "preHook";
    public static final String POST_HOOK_METRIC_NAME = "postHook";

    @StepHook(hookType = StepHook.HookType.PRE)
    public void preStepHook(MetricRecorder metrics) {
        metrics.addCount(PRE_HOOK_METRIC_NAME, 1.0);
    }

    @StepHook(hookType = StepHook.HookType.POST)
    public void postStepHook(MetricRecorder metrics) {
        metrics.addCount(POST_HOOK_METRIC_NAME, 1.0);
    }

}
