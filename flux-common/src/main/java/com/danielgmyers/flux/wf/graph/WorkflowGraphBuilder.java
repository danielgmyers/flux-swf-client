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

package com.danielgmyers.flux.wf.graph;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.CloseWorkflow;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepHook;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepHook;
import com.danielgmyers.flux.step.internal.WorkflowStepUtil;
import com.danielgmyers.metrics.MetricRecorder;

/**
 * Helper to build a WorkflowGraph object without needing to figure out its internals.
 */
public class WorkflowGraphBuilder {

    private final WorkflowStep firstStep;

    private final Map<Class<? extends WorkflowStep>, Map<String, Class<? extends WorkflowStep>>> steps;
    private final Map<Class<? extends WorkflowStep>, WorkflowStep> stepImpls;
    private final Map<String, Class<?>> initialAttributes;
    private final Map<Class<? extends WorkflowStep>, List<WorkflowStepHook>> stepHooks;
    private final List<WorkflowStepHook> hooksForAllSteps;
    private final Map<StepHook.HookType, List<WorkflowStepHook>> workflowHooksByType;
    private boolean shouldValidateAttributeAvailability;

    /**
     * Constructs a WorkflowGraphBuilder object.
     * @param firstStep The first step of the workflow. Must not be null.
     */
    public WorkflowGraphBuilder(WorkflowStep firstStep) {
        this(firstStep, Collections.emptyMap());
        this.shouldValidateAttributeAvailability = false;
    }

    /**
     * Constructs a WorkflowGraphBuilder object.
     * @param firstStep The first step of the workflow. Must not be null.
     * @param initialAttributes A map of attribute names and their types that are passed to the first step of the workflow.
     *                          If specified, the graph builder will validate step input attribute availability.
     */
    public WorkflowGraphBuilder(WorkflowStep firstStep, Map<String, Class<?>> initialAttributes) {
        this.firstStep = firstStep;
        this.steps = new HashMap<>();
        this.stepImpls = new HashMap<>();
        addStep(this.firstStep);
        this.stepHooks = new HashMap<>();
        this.hooksForAllSteps = new LinkedList<>();
        this.workflowHooksByType = new HashMap<>();
        this.shouldValidateAttributeAvailability = true;

        this.initialAttributes = new HashMap<>(initialAttributes);
        this.initialAttributes.put(StepAttributes.WORKFLOW_ID,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.WORKFLOW_ID));
        this.initialAttributes.put(StepAttributes.WORKFLOW_EXECUTION_ID,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.WORKFLOW_EXECUTION_ID));
        this.initialAttributes.put(StepAttributes.WORKFLOW_START_TIME,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.WORKFLOW_START_TIME));
    }

    /**
     * Adds a WorkflowStep object to the builder's metadata.
     * At least one transition must be defined for the step by calling one of the transition methods.
     * @param step The WorkflowStep object to add to the builder. Only one step of a given WorkflowStep implementation may be added.
     * @return A reference to the WorkflowGraphBuilder object.
     */
    public WorkflowGraphBuilder addStep(WorkflowStep step) {
        if (step == null) {
            throw new WorkflowGraphBuildException("Cannot add a null step.");
        }

        if (CloseWorkflow.class.isAssignableFrom(step.getClass())) {
            throw new WorkflowGraphBuildException("CloseWorkflow must not be implemented.");
        }

        if (stepImpls.containsKey(step.getClass())) {
            throw new WorkflowGraphBuildException("WorkflowStep classes can only be used once per workflow.");
        }

        Method applyMethod;
        try {
            applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
        } catch (Exception e) {
            throw new WorkflowGraphBuildException(e.getMessage(), e);
        }

        boolean requestsPartitionId = false;
        boolean requestsPartitionCount = false;

        for (Parameter param : applyMethod.getParameters()) {
            if (param.getType().isAssignableFrom(MetricRecorder.class)) {
                continue;
            }

            Attribute attr = validateAttributeAnnotationPresent(step, applyMethod, param);
            if (StepAttributes.PARTITION_ID.equals(attr.value())) {
                requestsPartitionId = true;
                if (!String.class.equals(param.getType())) {
                    throw new WorkflowGraphBuildException("The partition id attribute must be of type String.");
                }
            } else if (StepAttributes.PARTITION_COUNT.equals(attr.value())) {
                if (!Long.class.equals(param.getType())) {
                    throw new WorkflowGraphBuildException("The partition count attribute must be of type Long.");
                }
                requestsPartitionCount = true;
            } else {
                StepAttributes.validateAttributeClass(param.getType());
            }
        }

        if (PartitionedWorkflowStep.class.isAssignableFrom(step.getClass())) {
            if (!requestsPartitionId) {
                String message = String.format("Partitioned workflow step %s must have a parameter for the partition id.",
                                               step.getClass().getSimpleName());
                throw new WorkflowGraphBuildException(message);
            }

            // Ensure that the return type of the method is List<> or PartitionIdGeneratorResult.
            try {
                Method partitionIdGeneratorMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(),
                                                                                              PartitionIdGenerator.class);
                if ( !PartitionIdGeneratorResult.class.equals(partitionIdGeneratorMethod.getReturnType())) {
                    throw new WorkflowGraphBuildException(
                            String.format("%s.%s must have a return type of PartitionIdGeneratorResult.",
                                          step.getClass().getSimpleName(), partitionIdGeneratorMethod.getName()));
                }

                for (Parameter param : partitionIdGeneratorMethod.getParameters()) {
                    if (param.getType().isAssignableFrom(MetricRecorder.class)) {
                        continue;
                    }

                    Attribute attr = validateAttributeAnnotationPresent(step, partitionIdGeneratorMethod, param);
                    if (StepAttributes.PARTITION_ID.equals(attr.value()) || StepAttributes.PARTITION_COUNT.equals(attr.value())) {
                        String msg = String.format("%s.%s cannot request the PARTITION_ID or PARTITION_COUNT attributes.",
                                                   step.getClass().getSimpleName(), partitionIdGeneratorMethod.getName());
                        throw new WorkflowGraphBuildException(msg);
                    } else {
                        StepAttributes.validateAttributeClass(param.getType());
                    }
                }
            } catch (Exception e) {
                throw new WorkflowGraphBuildException(e.getMessage(), e);
            }
        } else if (requestsPartitionId || requestsPartitionCount) { // step is not partitioned
            String message = String.format("Workflow step %s is not partitioned but requested partition id or partition count.",
                                           step.getClass().getSimpleName());
            throw new WorkflowGraphBuildException(message);
        }

        stepImpls.put(step.getClass(), step);

        return this;
    }

    private Attribute validateAttributeAnnotationPresent(WorkflowStep step, Method method, Parameter param) {
        if (!param.isAnnotationPresent(Attribute.class)) {
            throw new WorkflowGraphBuildException(String.format("%s.%s parameter %s must have the @Attribute annotation.",
                    step.getClass().getSimpleName(),
                    method.getName(),
                    param.getName()));
        }
        Attribute attr = param.getAnnotation(Attribute.class);
        if (attr.value().equals("")) {
            String message = String.format("The @Attribute value for %s.%s parameter %s must not be blank.",
                    step.getClass().getSimpleName(),
                    method.getName(),
                    param.getName());
            throw new WorkflowGraphBuildException(message);
        }
        return attr;
    }

    /**
     * A shortcut for defining common transitions (success and failure) quickly.
     */
    public WorkflowGraphBuilder commonTransitions(WorkflowStep step, WorkflowStep onSuccess, WorkflowStep onFailure) {
        return successTransition(step, onSuccess).failTransition(step, onFailure);
    }

    /**
     * Closes the workflow if the step succeeds. Equivalent to successTransition(step.getClass(), CloseWorkflow.class);
     */
    public WorkflowGraphBuilder closeOnSuccess(WorkflowStep step) {
        return customTransition(step.getClass(), StepResult.SUCCEED_RESULT_CODE, CloseWorkflow.class);
    }

    /**
     * Closes the workflow if the step fails. Equivalent to failTransition(step.getClass(), CloseWorkflow.class);
     */
    public WorkflowGraphBuilder closeOnFailure(WorkflowStep step) {
        return customTransition(step.getClass(), StepResult.FAIL_RESULT_CODE, CloseWorkflow.class);
    }

    /**
     * Closes the workflow if the step returns a specific code. Equivalent to custom(step.getClass(), resultCode);
     */
    public WorkflowGraphBuilder closeOnCustom(WorkflowStep step, String resultCode) {
        return customTransition(step.getClass(), resultCode, CloseWorkflow.class);
    }

    /**
     * Closes the workflow regardless of the result code. Cannot call both alwaysClose() and closeOn*().
     */
    public WorkflowGraphBuilder alwaysClose(WorkflowStep step) {
        return customTransition(step.getClass(), StepResult.ALWAYS_RESULT_CODE, CloseWorkflow.class);
    }

    /**
     * Adds a transition from the specified step to the specified target step, using the default success result code.
     * Equivalent to the other overload, but extracts the Class objects automatically from specific WorkflowStep objects.
     */
    public WorkflowGraphBuilder successTransition(WorkflowStep step, WorkflowStep nextStep) {
        return customTransition(step.getClass(), StepResult.SUCCEED_RESULT_CODE, nextStep.getClass());
    }

    /**
     * Adds a transition from the specified step to the specified target step, using the default success result code.
     * @param step The source step for the transition.
     * @param nextStep The target step for the transition. Specify CloseWorkflow.class to end the workflow.
     * @return A reference to the WorkflowGraphBuilder object.
     */
    public WorkflowGraphBuilder successTransition(Class<? extends WorkflowStep> step, Class<? extends WorkflowStep> nextStep) {
        return customTransition(step, StepResult.SUCCEED_RESULT_CODE, nextStep);
    }

    /**
     * Adds a transition from the specified step to the specified target step, using the default failure result code.
     * Equivalent to the other overload, but extracts the Class objects automatically from specific WorkflowStep objects.
     */
    public WorkflowGraphBuilder failTransition(WorkflowStep step, WorkflowStep nextStep) {
        return customTransition(step.getClass(), StepResult.FAIL_RESULT_CODE, nextStep.getClass());
    }

    /**
     * Adds a transition from the specified step to the specified target step, using the default failure result code.
     * @param step The source step for the transition.
     * @param nextStep The target step for the transition. Specify CloseWorkflow.class to end the workflow.
     * @return A reference to the WorkflowGraphBuilder object.
     */
    public WorkflowGraphBuilder failTransition(Class<? extends WorkflowStep> step, Class<? extends WorkflowStep> nextStep) {
        return customTransition(step, StepResult.FAIL_RESULT_CODE, nextStep);
    }

    /**
     * Causes the workflow to *always* transition from the source step to the target step (still allowing retries).
     * Cannot specify both "always" and another transition.
     * Equivalent to the other overload, but extracts the Class objects automatically from specific WorkflowStep objects.
     */
    public WorkflowGraphBuilder alwaysTransition(WorkflowStep step, WorkflowStep nextStep) {
        return customTransition(step.getClass(), StepResult.ALWAYS_RESULT_CODE, nextStep.getClass());
    }

    /**
     * Causes the workflow to *always* transition from the source step to the target step (still allowing retries).
     * Cannot specify both "always" and another transition.
     * @param step The source step for the transition.
     * @param nextStep The target step for the transition. Specify CloseWorkflow.class to end the workflow.
     * @return A reference to the WorkflowGraphBuilder object.
     */
    public WorkflowGraphBuilder alwaysTransition(Class<? extends WorkflowStep> step, Class<? extends WorkflowStep> nextStep) {
        return customTransition(step, StepResult.ALWAYS_RESULT_CODE, nextStep);
    }

    /**
     * Adds a transition from the specified step to the specified target step, using a specific result code.
     * @param step The source step for the transition.
     * @param resultCode The result code to use for this transition. Must not be blank.
     * @param nextStep The target step for the transition. Specify CloseWorkflow.class to end the workflow.
     * @return A reference to the WorkflowGraphBuilder object.
     */
    public WorkflowGraphBuilder customTransition(Class<? extends WorkflowStep> step, String resultCode,
                                                 Class<? extends WorkflowStep> nextStep) {
        if (!stepImpls.containsKey(step)) {
            throw new WorkflowGraphBuildException("Please add a step with addStep before defining transitions away from it.");
        }

        if (nextStep == null) {
            throw new WorkflowGraphBuildException("Cannot transition to a null step."
                                                  + " (Did you mean to transition to CloseWorkflow.class?)");
        } else if (nextStep == step) {
            throw new WorkflowGraphBuildException("Cannot transition a step to itself (the step should retry instead).");
        }

        if (resultCode == null || resultCode.equals("")) {
            throw new WorkflowGraphBuildException("Result codes must not be blank.");
        }

        if (PartitionedWorkflowStep.class.isAssignableFrom(step)
                && !StepResult.VALID_PARTITIONED_STEP_RESULT_CODES.contains(resultCode)) {
            throw new WorkflowGraphBuildException("Partitioned steps may not define custom result codes.");
        }

        if (!steps.containsKey(step)) {
            steps.put(step, new HashMap<>());
        } else if (steps.get(step).containsKey(resultCode)) {
            throw new WorkflowGraphBuildException("Multiple transitions cannot be defined for a single result code ("
                                                  + resultCode + ").");
        } else if (!steps.get(step).isEmpty() && (StepResult.ALWAYS_RESULT_CODE.equals(resultCode)
                                                  || steps.get(step).containsKey(StepResult.ALWAYS_RESULT_CODE))) {
            throw new WorkflowGraphBuildException("Cannot define 'always' and another transition for the same step.");
        }

        steps.get(step).put(resultCode, nextStep);

        return this;
    }

    /**
     * Delegates to the other overload, extracting the Class objects automatically from specific WorkflowStep objects.
     */
    public WorkflowGraphBuilder customTransition(WorkflowStep step, String resultCode, WorkflowStep nextStep) {
        return customTransition(step.getClass(), resultCode, nextStep.getClass());
    }

    /**
     * Adds a step hook to an individual step. Adding the same hook more than once is allowed.
     */
    public WorkflowGraphBuilder addStepHook(WorkflowStep stepToHook, WorkflowStepHook hook) {
        Class<? extends WorkflowStep> stepClass = stepToHook.getClass();
        if (!stepImpls.containsKey(stepClass)) {
            throw new WorkflowGraphBuildException("Please add a step with addStep before adding hooks for it.");
        }

        validateHook(hook);

        if (!stepHooks.containsKey(stepClass)) {
            stepHooks.put(stepClass, new LinkedList<>());
        }
        stepHooks.get(stepClass).add(hook);
        return this;
    }

    /**
     * Adds a collection of step hooks to an individual step. The collection may contain duplicates.
     *
     * Private because it skips validating the @StepHook methods.
     */
    private WorkflowGraphBuilder addStepHooks(Class<? extends WorkflowStep> stepToHook, Collection<WorkflowStepHook> hooks) {
        if (!stepImpls.containsKey(stepToHook)) {
            throw new WorkflowGraphBuildException("Please add a step with addStep before adding hooks for it.");
        }

        if (hooks == null || hooks.isEmpty()) {
            throw new WorkflowGraphBuildException("Cannot add zero hooks.");
        }

        if (!stepHooks.containsKey(stepToHook)) {
            stepHooks.put(stepToHook, new LinkedList<>());
        }
        stepHooks.get(stepToHook).addAll(hooks);
        return this;
    }

    /**
     * Adds a step hook that should be used for all steps. Adding the same hook more than once is allowed.
     */
    public WorkflowGraphBuilder addHookForAllSteps(WorkflowStepHook hook) {
        validateHook(hook);
        hooksForAllSteps.add(hook);
        return this;
    }

    /**
     * Adds a step hook to the workflow itself. Adding the same hook more than once is allowed.
     * - PRE hooks are executed before the first workflow step.
     * - POST hooks are executed after the workflow terminates. If it is a @Periodic workflow, executes before delayExit.
     */
    public WorkflowGraphBuilder addWorkflowHook(WorkflowStepHook hook) {
        Map<StepHook.HookType, Method> methodsByType = validateHook(hook);
        for (StepHook.HookType hookType : methodsByType.keySet()) {
            if (!workflowHooksByType.containsKey(hookType)) {
                workflowHooksByType.put(hookType, new LinkedList<>());
            }
            workflowHooksByType.get(hookType).add(hook);
        }
        return this;
    }

    private Map<StepHook.HookType, Method> validateHook(WorkflowStepHook hook) {
        if (hook == null) {
            throw new WorkflowGraphBuildException("Cannot add a null hook.");
        }

        Map<Method, StepHook> hookMethods = WorkflowStepUtil.getAllMethodsWithAnnotation(hook.getClass(), StepHook.class);
        if (hookMethods.isEmpty()) {
            throw new WorkflowGraphBuildException("Step hooks must have at least one @StepHook method.");
        }
        Map<StepHook.HookType, Method> hookMethodsByType = new HashMap<>();
        for (Entry<Method, StepHook> method : hookMethods.entrySet()) {
            if (hookMethodsByType.containsKey(method.getValue().hookType())) {
                throw new WorkflowGraphBuildException(String.format("Hook %s cannot have more than one hook of type %s.",
                                                                    hook.getClass().getSimpleName(), method.getValue().hookType()));
            }
            hookMethodsByType.put(method.getValue().hookType(), method.getKey());
        }
        return hookMethodsByType;
    }

    /**
     * Validates and builds the WorkflowGraph representing the steps and transitions added to the builder so far.
     * @return A WorkflowGraph object.
     */
    public WorkflowGraph build() {
        WorkflowStep actualFirstStep = firstStep;
        // if we have any pre-workflow hooks, insert the pre-workflow-hook anchor step and assign the workflow hooks to it.
        List<WorkflowStepHook> preWorkflowHooks = workflowHooksByType.get(StepHook.HookType.PRE);
        if (preWorkflowHooks != null && !preWorkflowHooks.isEmpty()) {
            actualFirstStep = new PreWorkflowHookAnchor();
            addStep(actualFirstStep);
            alwaysTransition(actualFirstStep, firstStep);
            addStepHooks(PreWorkflowHookAnchor.class, preWorkflowHooks);
        }

        // verify that all of the steps in stepImpls have behavior defined in the steps map
        if (!steps.keySet().containsAll(stepImpls.keySet())) {
            Set<Class<? extends WorkflowStep>> diff = new HashSet<>(stepImpls.keySet());
            diff.removeAll(steps.keySet());

            Set<String> names = diff.stream().map(Class::getSimpleName).collect(Collectors.toSet());
            throw new WorkflowGraphBuildException("Found one or more steps with undefined behavior: " + String.join(", ", names));
        }

        // if we have any post-workflow hooks, add the post-workflow-hook anchor step and assign the workflow hooks to it.
        // we will add transitions to it below when we encounter CloseWorkflow transitions.
        List<WorkflowStepHook> postWorkflowHooks = workflowHooksByType.get(StepHook.HookType.POST);
        if (postWorkflowHooks != null && !postWorkflowHooks.isEmpty()) {
            addStep(new PostWorkflowHookAnchor());
            addStepHooks(PostWorkflowHookAnchor.class, postWorkflowHooks);

            // we'll force all of the CloseWorkflow transitions to PostWorkflowHookAnchor instead
            for (Class<? extends WorkflowStep> step : steps.keySet()) {
                for (String resultCode : steps.get(step).keySet()) {
                    if (steps.get(step).get(resultCode) == CloseWorkflow.class) {
                        steps.get(step).put(resultCode, PostWorkflowHookAnchor.class);
                    }
                }
            }

            // finally we'll add in the transition from the anchor to CloseWorkflow.
            alwaysTransition(PostWorkflowHookAnchor.class, CloseWorkflow.class);
        }

        Map<Class<? extends WorkflowStep>, WorkflowGraphNode> nodes = new HashMap<>();
        for (Entry<Class<? extends WorkflowStep>, WorkflowStep> entry : stepImpls.entrySet()) {
            nodes.put(entry.getKey(), new WorkflowGraphNodeImpl(entry.getValue()));
        }

        Set<Class<? extends WorkflowStep>> reachable = new HashSet<>();
        reachable.add(actualFirstStep.getClass()); // the first step is always reachable

        for (Class<? extends WorkflowStep> step : steps.keySet()) {
            WorkflowGraphNode node = nodes.get(step);
            // now look up the nodes for each of the transitions defined for this step,
            // and add a transition entry for it to this step's node
            for (String resultCode : steps.get(step).keySet()) {
                Class<? extends WorkflowStep> nextStep = steps.get(step).get(resultCode);
                if (CloseWorkflow.class.isAssignableFrom(nextStep)) {
                    // if the transition is to CloseWorkflow, we put a null transition in so we end the workflow
                    node.addTransition(resultCode, null);
                } else if (!nodes.containsKey(nextStep)) {
                    throw new WorkflowGraphBuildException("A transition to step " + nextStep.getSimpleName()
                                                          + " was defined from " + step.getSimpleName()
                                                          + " but no behavior was specified for " + nextStep.getSimpleName());
                } else {
                    WorkflowGraphNode nextNode = nodes.get(nextStep);
                    node.addTransition(resultCode, nextNode);
                    reachable.add(nextNode.getStep().getClass());
                }
            }
        }

        // verify that there are no unreachable nodes in the graph
        if (!reachable.containsAll(steps.keySet())) {
            Set<Class<? extends WorkflowStep>> diff = new HashSet<>(steps.keySet());
            diff.removeAll(reachable);

            Set<String> names = diff.stream().map(Class::getSimpleName).collect(Collectors.toSet());
            throw new WorkflowGraphBuildException("Found one or more unreachable steps: " + String.join(", ", names));
        }

        verifyNoLoops(actualFirstStep.getClass(), nodes, new HashSet<>());

        // now we'll loop through hooksForAllSteps and add each entry to every step
        if (!hooksForAllSteps.isEmpty()) {
            for (Class<? extends WorkflowStep> step : steps.keySet()) {
                addStepHooks(step, hooksForAllSteps);
            }
        }

        if (shouldValidateAttributeAvailability) {
            validateAttributeAvailability(actualFirstStep, nodes, initialAttributes, stepHooks);
        }

        return new WorkflowGraphImpl(actualFirstStep, nodes, stepHooks);
    }

    private void verifyNoLoops(Class<? extends WorkflowStep> step,
                               Map<Class<? extends WorkflowStep>, WorkflowGraphNode> nodes,
                               Set<Class<? extends WorkflowStep>> visited) {
        visited.add(step);

        for (WorkflowGraphNode node : nodes.get(step).getNextStepsByResultCode().values()) {
            if (node == null) {
                // this means the workflow ends for that result code, move on to the next.
                continue;
            }

            if (visited.contains(node.getStep().getClass())) {
                throw new WorkflowGraphBuildException("The graph contains a loop (includes step "
                                                      + node.getStep().getClass().getSimpleName() + ")");
            }

            verifyNoLoops(node.getStep().getClass(), nodes, visited);
        }

        // if we get here, there were no loops including this node
        visited.remove(step);
    }

    /**
     * Assumes there are no loops in the graph.
     */
    private void validateAttributeAvailability(WorkflowStep step,
                                               Map<Class<? extends WorkflowStep>, WorkflowGraphNode> nodes,
                                               Map<String, Class<?>> availableAttributes,
                                               Map<Class<? extends WorkflowStep>, List<WorkflowStepHook>> stepHooks) {
        boolean stepIsPartitioned = PartitionedWorkflowStep.class.isAssignableFrom(step.getClass());
        if (stepIsPartitioned) {
            Method partitionIdGenerator = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), PartitionIdGenerator.class);
            for (Parameter param : partitionIdGenerator.getParameters()) {
                if (param.getType().isAssignableFrom(MetricRecorder.class)) {
                    continue;
                }

                Attribute attr = param.getAnnotation(Attribute.class);
                if (StepAttributes.PARTITION_ID.equals(attr.value()) || StepAttributes.PARTITION_COUNT.equals(attr.value())) {
                    String msg = String.format("%s.%s cannot request the PARTITION_ID or PARTITION_COUNT attributes.",
                                               step.getClass().getSimpleName(), partitionIdGenerator.getName());
                    throw new WorkflowGraphBuildException(msg);
                }

                Class<?> paramType = param.getType();
                validateAttributeIdAndType(step.getClass().getSimpleName(), partitionIdGenerator.getName(), attr.value(),
                                           paramType, attr.optional(), availableAttributes);
            }

            addDeclaredOutputAttributes(availableAttributes, step);
        }

        Map<String, Class<?>> withSpecialAttributes = new HashMap<>(availableAttributes);
        withSpecialAttributes.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME,
                                  StepAttributes.getSpecialAttributeType(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
        withSpecialAttributes.put(StepAttributes.RETRY_ATTEMPT,
                                  StepAttributes.getSpecialAttributeType(StepAttributes.RETRY_ATTEMPT));
        if (stepIsPartitioned) {
            withSpecialAttributes.put(StepAttributes.PARTITION_ID,
                                      StepAttributes.getSpecialAttributeType(StepAttributes.PARTITION_ID));
            withSpecialAttributes.put(StepAttributes.PARTITION_COUNT,
                                      StepAttributes.getSpecialAttributeType(StepAttributes.PARTITION_COUNT));
        }

        Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
        for (Parameter param : applyMethod.getParameters()) {
            Class<?> paramType = param.getType();
            if (paramType.isAssignableFrom(MetricRecorder.class)) {
                continue;
            }

            Attribute attr = param.getAnnotation(Attribute.class);
            validateAttributeIdAndType(step.getClass().getSimpleName(), applyMethod.getName(), attr.value(), paramType,
                                       attr.optional(), withSpecialAttributes);
        }

        Map<String, Class<?>> nextStepAttributes = new HashMap<>(availableAttributes);
        if (!stepIsPartitioned) {
            addDeclaredOutputAttributes(nextStepAttributes, step);
        }

        if (stepHooks.containsKey(step.getClass())) {
            for (WorkflowStepHook hook : stepHooks.get(step.getClass())) {
                validateHookAttributeAvailability(step, hook, StepHook.HookType.PRE, availableAttributes, stepIsPartitioned);
                validateHookAttributeAvailability(step, hook, StepHook.HookType.POST, nextStepAttributes, stepIsPartitioned);
            }
        }

        for (WorkflowGraphNode node : nodes.get(step.getClass()).getNextStepsByResultCode().values()) {
            if (node == null) {
                // this means the workflow ends for that result code, move on to the next.
                continue;
            }
            validateAttributeAvailability(node.getStep(), nodes, nextStepAttributes, stepHooks);
        }
    }

    private void addDeclaredOutputAttributes(Map<String, Class<?>> attributes, WorkflowStep step) {
        for (Entry<String, Class<?>> entry : step.declaredOutputAttributes().entrySet()) {
            if (attributes.containsKey(entry.getKey())) {
                String msg = String.format("Workflow step %s overwrites previously returned attribute %s.",
                                           step.getClass().getSimpleName(), entry.getKey());
                throw new WorkflowGraphBuildException(msg);
            }
            attributes.put(entry.getKey(), entry.getValue());
        }
    }

    private void validateHookAttributeAvailability(WorkflowStep hookedStep, WorkflowStepHook hook, StepHook.HookType hookType,
                                                   Map<String, Class<?>> availableAttributes, boolean allowPartitionAttributes) {
        Map<Method, StepHook> methods = WorkflowStepUtil.getAllMethodsWithAnnotation(hook.getClass(), StepHook.class);
        for (Entry<Method, StepHook> entry : methods.entrySet()) {
            StepHook config = entry.getValue();
            if (config.hookType() != hookType) {
                continue;
            }

            Map<String, Class<?>> hookAttributes = new HashMap<>(availableAttributes);
            hookAttributes.put(StepAttributes.ACTIVITY_NAME,
                               StepAttributes.getSpecialAttributeType(StepAttributes.ACTIVITY_NAME));
            hookAttributes.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME,
                               StepAttributes.getSpecialAttributeType(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME));
            hookAttributes.put(StepAttributes.RETRY_ATTEMPT,
                               StepAttributes.getSpecialAttributeType(StepAttributes.RETRY_ATTEMPT));
            if (allowPartitionAttributes) {
                hookAttributes.put(StepAttributes.PARTITION_ID,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.PARTITION_ID));
                hookAttributes.put(StepAttributes.PARTITION_COUNT,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.PARTITION_COUNT));
            }
            if (hookType == StepHook.HookType.POST) {
                hookAttributes.put(StepAttributes.RESULT_CODE,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.RESULT_CODE));
                hookAttributes.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE,
                                   StepAttributes.getSpecialAttributeType(StepAttributes.ACTIVITY_COMPLETION_MESSAGE));
            }

            Method method = entry.getKey();
            for (Parameter param : method.getParameters()) {
                Class<?> paramType = param.getType();
                if (paramType.isAssignableFrom(MetricRecorder.class)) {
                    continue;
                }

                Attribute attr = param.getAnnotation(Attribute.class);
                if (!allowPartitionAttributes && (StepAttributes.PARTITION_ID.equals(attr.value())
                                                  || StepAttributes.PARTITION_COUNT.equals(attr.value()))) {
                    String msg = String.format("%s.%s cannot request the PARTITION_ID or PARTITION_COUNT attributes for step %s.",
                                               hook.getClass().getSimpleName(), method.getName(),
                                               hookedStep.getClass().getSimpleName());
                    throw new WorkflowGraphBuildException(msg);
                }

                validateAttributeIdAndType(hook.getClass().getSimpleName(), method.getName(), attr.value(),
                                           paramType, attr.optional(), hookAttributes);
            }
        }
    }

    private void validateAttributeIdAndType(String stepName, String methodName, String attributeName, Class<?> actualType,
                                            boolean optional, Map<String, Class<?>> availableAttributes) {
        if (!availableAttributes.containsKey(attributeName) && !optional) {
            String msg = String.format("%s.%s requires attribute %s but it is not available in at least one path.",
                                       stepName, methodName, attributeName);
            throw new WorkflowGraphBuildException(msg);
        }
        if (availableAttributes.containsKey(attributeName) && !actualType.equals(availableAttributes.get(attributeName))) {
            String msg = String.format("%s.%s expects attribute %s to have type %s but it has type %s.",
                                       stepName, methodName, attributeName, actualType.getSimpleName(),
                                       availableAttributes.get(attributeName).getSimpleName());
            throw new WorkflowGraphBuildException(msg);
        }
    }
}
