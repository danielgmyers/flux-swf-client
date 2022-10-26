package com.danielgmyers.flux.wf.graph;

import java.util.Map;

import com.danielgmyers.flux.step.WorkflowStep;

public interface WorkflowGraphNode {
    WorkflowStep getStep();

    Map<String, WorkflowGraphNode> getNextStepsByResultCode();

    // only for WorkflowGraphBuilder internal use. If node is null, the workflow will end.
    void addTransition(String resultCode, WorkflowGraphNode node);
}
