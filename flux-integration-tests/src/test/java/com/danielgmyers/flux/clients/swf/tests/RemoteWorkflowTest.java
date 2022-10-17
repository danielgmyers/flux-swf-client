package com.danielgmyers.flux.clients.swf.tests;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.clients.swf.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.RemoteWorkflowExecutor;
import com.danielgmyers.flux.clients.swf.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @BeforeEach
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
        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

        Assertions.assertEquals(WorkflowStatusChecker.WorkflowStatus.IN_PROGRESS, status.checkStatus());

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
