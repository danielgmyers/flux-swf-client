package software.amazon.aws.clients.swf.flux.tests;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.FluxCapacitor;
import software.amazon.aws.clients.swf.flux.RemoteWorkflowExecutor;
import software.amazon.aws.clients.swf.flux.WorkflowStatusChecker;
import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Validates we can run workflows against a remote region.
 */
public class RemoteWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new HelloWorld());
    }

    /**
     * Ensures the workflow type is registered in the remote region.
     */
    @Before
    public void setUpRemoteWorkflows() throws InterruptedException {
        // This is probably suboptimal but here all we're trying to do is ensure our workflows are registered in the remote region.
        FluxCapacitor remoteCapacitor = createFluxCapacitor(false, getWorkflowsForTest());

        // that's all we needed, let's shut down now...
        log.info("Shutting down remote Flux with domain " + getWorkflowDomain() + "...");
        remoteCapacitor.shutdown();
        remoteCapacitor.awaitTermination(5, TimeUnit.SECONDS);
        log.info("Remote Flux shutdown complete for domain " + getWorkflowDomain() + ".");
    }

    /**
     * Tests that we can request a workflow against a remote region.
     * Note that this test doesn't actually try to *finish* the remote workflow, only request execution and then terminate it.
     */
    @Test
    public void testStartRemoteWorkflow() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        log.info("Requesting execution of remote workflow...");
        RemoteWorkflowExecutor remoteExecutor = getRemoteWorkflowExecutor();
        WorkflowStatusChecker status = remoteExecutor.executeWorkflow(HelloWorld.class, uuid, Collections.emptyMap());
        WorkflowStatusChecker.WorkflowStatus lastStatus = status.checkStatus();
        log.info("Received status " + lastStatus.toString() + " for requested remote workflow.");

        WorkflowExecutionInfo info = status.getExecutionInfo();
        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assert.assertEquals(WorkflowStatusChecker.WorkflowStatus.IN_PROGRESS, status.checkStatus());

        terminateOpenWorkflowExecutions(status.getSwfClient());
    }

    /**
     * Basic workflow with one step.
     */
    public static final class HelloWorld implements Workflow {
        private final WorkflowGraph graph;

        HelloWorld() {
            WorkflowStep stepOne = new StepOne();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.alwaysClose(stepOne);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that records which workflow IDs the step is executed for.
     */
    public static final class StepOne implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
