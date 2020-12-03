/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.aws.clients.swf.flux.step;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tells FluxCapacitor which method should be called as a hook around a WorkflowStep's @StepApply method.
 *
 * The parameters of the method should have the @Attribute annotation.
 *
 * A small number of attributes are available to hooks in addition to the attributes from the running workflow:
 * - StepAttributes.WORKFLOW_ID - The unique identifier of this workflow execution,
 *                                i.e. what was passed to FluxCapacitor.executeWorkflow. This must be a String.
 * - StepAttributes.ACTIVITY_NAME - The name of this activity (in the format {workflow-name}.{step-name}). This must be a String.
 *
 * POST hooks may also request the StepAttributes.RESULT_CODE and StepAttributes.ACTIVITY_COMPLETION_MESSAGE attributes;
 * they will both be null if the step is going to retry. Both are Strings.
 *
 * The hook is run every time the hooked step is run and should therefore be idempotent.
 *
 * The return value of the method is logged but otherwise ignored, so the return type does not matter.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepHook {

    /**
     * An enum indicating which type of hook this is:
     * - PRE: the method should be called before the @StepApply method of the hooked step.
     * - POST: the method should be called after the @StepApply method of the hooked step.
     */
    enum HookType {
        PRE, POST
    }

    /**
     * Required. A class implementing WorkflowStepHook may have up to one hook method of each type.
     */
    HookType hookType();

    /**
     * If this is true, and the method throws an exception, then Flux will cause the hooked step to retry.
     * (Note that in that case, if this is a PRE_HOOK, the step's @StepApply method will not run.)
     *
     * Emitting metrics, informational logging, alarming, or other non-essential actions generally do not merit enabling this flag.
     * Important functions such as audit logging may merit enabling the flag.
     */
    boolean retryOnFailure() default false;
}
