package software.amazon.aws.clients.swf.flux.wf.graph;

import java.util.List;
import java.util.Map;

import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepHook;

public interface WorkflowGraph {
    WorkflowStep getFirstStep();

    Map<Class<? extends WorkflowStep>, WorkflowGraphNode> getNodes();

    List<WorkflowStepHook> getHooksForStep(Class<? extends WorkflowStep> step);
}
