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

package com.danielgmyers.flux.clients.swf.step;

/**
 * An interface marking a class to be a workflow step hook (i.e. something that is run before or after a step).
 * The class must define at least one method with the @StepHook annotation.
 *
 * Step hooks can be added using WorkflowGraphBuilder's addStepHook(), addHookForAllSteps(), or addWorkflowHook() methods.
 *
 * Step hooks are a mechanism to run arbitrary code before or after an actual workflow step is run.
 * For example, a hook might be added before each step that cuts a ticket to an operator if the step has been
 * running for more than 1 hour, or if it has retried too many times.
 *
 * Step hooks may also be attached to a workflow, rather than a step within a workflow; in this case, the
 * hook code is executed either before the first workflow step, or after the last workflow step (regardless
 * of which step was actually executed last).
 *
 * There are two types of hooks:
 * - PRE hooks run before a workflow step's @StepApply method.
 * - POST hooks run after the workflow step's @StepApply method, even if it threw an exception.
 *
 * When executing a workflow step, Flux first executes any PRE hooks, then executes the workflow
 * step's @StepApply method, and finally executes any POST hooks.
 *
 * A step hook has access to the same set of input attributes as the workflow step it is attached to.
 * PRE and POST hooks both have access to these attributes:
 *
 * - StepAttributes.ACTIVITY_NAME: String
 *   The name of the step being hooked, in the format "{workflow-name}.{step-name}".
 *   Only available to workflow step hooks, not workflow hooks.
 *
 * POST step hooks also have access to the step's output attributes as well as these two attributes
 * if the step completed successfully:
 *
 * - StepAttributes.RESULT_CODE: (String)
 *   The result code returned by the step (or determined by Flux, if the step didn't return a StepResult object).
 *   This will be null if the step retried.
 * - StepAttributes.ACTIVITY_COMPLETION_MESSAGE: (String)
 *   The message included in the StepResult object returned by the @StepApply method
 *   (or determined by Flux from a thrown Exception's message), if any.
 *   This will be null if no message was provided.
 *
 * Flux does not enforce a limit on the number of hooks that can be added to a workflow or a workflow step.
 * However, hooks are moderately difficult to debug, so it is recommended that only a minimum number of hooks be used.
 * Step hooks are run in the activity thread, after heartbeating has started, so they won't normally cause a step to
 * time out, but a workflow hook is executed by the decider thread, which does not heartbeat; if those hooks are expensive,
 * the decider may time out.
 *
 * If the same hook is added to a step more than once, it will execute more than once.
 */
public interface WorkflowStepHook {
}
