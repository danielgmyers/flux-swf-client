package com.danielgmyers.flux.wf.graph;

import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepHook;

public interface WorkflowGraph {
    WorkflowStep getFirstStep();

    Map<Class<? extends WorkflowStep>, WorkflowGraphNode> getNodes();

    List<WorkflowStepHook> getHooksForStep(Class<? extends WorkflowStep> step);
}
