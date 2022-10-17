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

package com.danielgmyers.flux.clients.swf.wf.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.step.WorkflowStepHook;

/**
 * A graph representation of how Flux should proceed through the workflow.
 * Construct one using WorkflowGraphBuilder.
 */
public final class WorkflowGraphImpl implements WorkflowGraph {

    private final WorkflowStep firstStep;
    private final Map<Class<? extends WorkflowStep>, WorkflowGraphNode> steps;
    private final Map<Class<? extends WorkflowStep>, List<WorkflowStepHook>> stepHooks;

    // package-private so only WorkflowGraphBuilder can create one
    WorkflowGraphImpl(WorkflowStep firstStep, Map<Class<? extends WorkflowStep>, WorkflowGraphNode> steps,
                      Map<Class<? extends WorkflowStep>, List<WorkflowStepHook>> stepHooks) {
        this.firstStep = firstStep;
        this.steps = Collections.unmodifiableMap(new HashMap<>(steps));

        Map<Class<? extends WorkflowStep>, List<WorkflowStepHook>> stepHooksCopy = new HashMap<>();
        for (Map.Entry<Class<? extends WorkflowStep>, List<WorkflowStepHook>> step : stepHooks.entrySet()) {
            stepHooksCopy.put(step.getKey(), Collections.unmodifiableList(new ArrayList<>(step.getValue())));
        }
        this.stepHooks = Collections.unmodifiableMap(stepHooksCopy);
    }

    @Override
    public WorkflowStep getFirstStep() {
        return firstStep;
    }

    @Override
    public Map<Class<? extends WorkflowStep>, WorkflowGraphNode> getNodes() {
        return steps;
    }

    @Override
    public List<WorkflowStepHook> getHooksForStep(Class<? extends WorkflowStep> step) {
        return stepHooks.get(step);
    }
}
