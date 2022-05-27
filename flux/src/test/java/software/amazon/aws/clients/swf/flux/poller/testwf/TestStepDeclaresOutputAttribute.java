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

package software.amazon.aws.clients.swf.flux.poller.testwf;

import java.util.HashMap;
import java.util.Map;

import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

public class TestStepDeclaresOutputAttribute implements WorkflowStep {

    @StepApply
    public StepResult apply() {
        return StepResult.success().withAttribute(TestStepHasInputAttribute.INPUT_ATTR, "foo");
    }

    @Override
    public Map<String, Class<?>> declaredOutputAttributes() {
        Map<String, Class<?>> attrs = new HashMap<>();
        attrs.put(TestStepHasInputAttribute.INPUT_ATTR, String.class);
        return attrs;
    }
}
