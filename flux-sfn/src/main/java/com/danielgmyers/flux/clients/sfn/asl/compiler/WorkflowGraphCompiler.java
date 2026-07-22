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

package com.danielgmyers.flux.clients.sfn.asl.compiler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.asl.StateMachineDefinition;
import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceRule;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceState;
import com.danielgmyers.flux.clients.sfn.asl.state.FailState;
import com.danielgmyers.flux.clients.sfn.asl.state.SucceedState;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;

/**
 * Compiles a Flux {@link WorkflowGraph} into an Amazon States Language {@link StateMachineDefinition}.
 *
 * <p>The compiler performs three phases:</p>
 * <ol>
 *   <li><b>Fragment building:</b> For each node in the graph, build a {@link StepFragment}.</li>
 *   <li><b>Transition resolution:</b> Wire each fragment's exit transitions to the appropriate
 *       target fragment's entry state name, or to a shared terminal Succeed state.</li>
 *   <li><b>Assembly:</b> Collect all states into a single {@link StateMachineDefinition}.</li>
 * </ol>
 */
public class WorkflowGraphCompiler {

    private final FluxCapacitorConfig config;

    public WorkflowGraphCompiler(FluxCapacitorConfig config) {
        this.config = config;
    }

    /**
     * Compiles the given workflow into a complete ASL state machine definition.
     *
     * @param workflow The Flux workflow to compile.
     * @return A StateMachineDefinition that can be serialized to ASL JSON.
     */
    public StateMachineDefinition compile(Workflow workflow) {
        Class<? extends Workflow> workflowClass = workflow.getClass();
        String workflowName = TaskNaming.workflowName(workflowClass);
        WorkflowGraph graph = workflow.getGraph();

        StepFragmentBuilder fragmentBuilder = new StepFragmentBuilder(workflowClass, config);

        // Phase 1: Build a fragment for each node
        Map<Class<? extends WorkflowStep>, StepFragment> fragments = new LinkedHashMap<>();
        for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : graph.getNodes().entrySet()) {
            fragments.put(entry.getKey(), fragmentBuilder.build(entry.getValue()));
        }

        // Phase 2: Resolve transitions
        String succeedStateName = workflowName + "." + StepFragment.SUFFIX_SUCCEED;
        String failedStateName = workflowName + "." + StepFragment.SUFFIX_FAILED;

        for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : graph.getNodes().entrySet()) {
            WorkflowGraphNode node = entry.getValue();
            StepFragment fragment = fragments.get(entry.getKey());
            Map<String, WorkflowGraphNode> transitions = node.getNextStepsByResultCode();

            resolveTransitions(fragment, transitions, fragments, succeedStateName, failedStateName);
        }

        // Phase 3: Assemble the state machine
        Map<String, AslState> allStates = new LinkedHashMap<>();
        for (StepFragment fragment : fragments.values()) {
            allStates.putAll(fragment.getStates());
        }

        // Add shared terminal states
        allStates.put(succeedStateName, new SucceedState());

        FailState workflowFailedState = new FailState();
        workflowFailedState.setError("Flux.WorkflowFailed");
        workflowFailedState.setCausePath("$." + StepAttributes.ACTIVITY_COMPLETION_MESSAGE);
        allStates.put(failedStateName, workflowFailedState);

        // Determine the start state from the graph's first step
        String startAt = fragments.get(graph.getFirstStep().getClass()).getEntryStateName();

        StateMachineDefinition definition = new StateMachineDefinition();
        definition.setStartAt(startAt);
        definition.setStates(allStates);
        definition.setComment(workflowName);

        return definition;
    }

    /**
     * Resolves the exit transitions of a fragment by setting the Next fields on the appropriate ASL states.
     */
    private void resolveTransitions(StepFragment fragment, Map<String, WorkflowGraphNode> transitions,
                                    Map<Class<? extends WorkflowStep>, StepFragment> fragments,
                                    String succeedStateName, String failedStateName) {
        Map<String, String> exitTransitions = fragment.getExitTransitionResultCodes();

        if (exitTransitions.containsKey(StepResult.ALWAYS_RESULT_CODE)) {
            // _always: the exit points to the entry Task state name; set its Next directly
            String taskStateName = exitTransitions.get(StepResult.ALWAYS_RESULT_CODE);
            AslState taskState = fragment.getStates().get(taskStateName);
            WorkflowGraphNode targetNode = transitions.get(StepResult.ALWAYS_RESULT_CODE);
            String targetStateName = resolveTargetStateName(StepResult.ALWAYS_RESULT_CODE, targetNode,
                                                            fragments, succeedStateName, failedStateName);

            if (taskState instanceof TaskState) {
                ((TaskState) taskState).setNext(targetStateName);
            } else if (taskState instanceof com.danielgmyers.flux.clients.sfn.asl.state.MapState) {
                ((com.danielgmyers.flux.clients.sfn.asl.state.MapState) taskState).setNext(targetStateName);
            }
        } else {
            // Branching: the exit transitions all point to the Route (Choice) state name.
            // We need to update the Choice rules' Next fields to point to the resolved targets.
            String routeStateName = exitTransitions.values().iterator().next();
            AslState routeState = fragment.getStates().get(routeStateName);

            if (routeState instanceof ChoiceState) {
                ChoiceState choiceState = (ChoiceState) routeState;
                List<ChoiceRule> rules = choiceState.getChoices();

                for (ChoiceRule rule : rules) {
                    String resultCode = rule.getStringEquals();
                    WorkflowGraphNode targetNode = transitions.get(resultCode);
                    String targetStateName = resolveTargetStateName(resultCode, targetNode,
                                                                    fragments, succeedStateName, failedStateName);
                    rule.setNext(targetStateName);
                }
            }
        }
    }

    /**
     * Resolves the target state name for a transition. If the target node is null (close workflow),
     * returns the shared Failed state if the result code is {@code _fail}, or the shared Succeed state otherwise.
     * If the target node is non-null, returns the target fragment's entry state name.
     */
    private String resolveTargetStateName(String resultCode, WorkflowGraphNode targetNode,
                                          Map<Class<? extends WorkflowStep>, StepFragment> fragments,
                                          String succeedStateName, String failedStateName) {
        if (targetNode == null) {
            if (StepResult.FAIL_RESULT_CODE.equals(resultCode)) {
                return failedStateName;
            }
            return succeedStateName;
        }
        StepFragment targetFragment = fragments.get(targetNode.getStep().getClass());
        return targetFragment.getEntryStateName();
    }
}
