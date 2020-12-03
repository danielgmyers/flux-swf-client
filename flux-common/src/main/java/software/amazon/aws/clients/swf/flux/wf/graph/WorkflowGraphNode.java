package software.amazon.aws.clients.swf.flux.wf.graph;

import java.util.Map;

import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

public interface WorkflowGraphNode {
    WorkflowStep getStep();

    Map<String, WorkflowGraphNode> getNextStepsByResultCode();

    // only for WorkflowGraphBuilder internal use. If node is null, the workflow will end.
    void addTransition(String resultCode, WorkflowGraphNode node);
}
