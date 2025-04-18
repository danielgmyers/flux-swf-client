package com.danielgmyers.flux.clients.swf.tests;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.SwfClient;

/**
 * Validates we can run workflows against a remote region.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class RemoteWorkflowTest extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowTest.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new HelloWorld());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    /**
     * Ensures the workflow type is registered in the remote region.
     */
    @BeforeAll
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
        WorkflowInfo info = status.getWorkflowInfo();
        log.info("Received status " + info.getWorkflowStatus() + " for requested remote workflow.");

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME), info.getExecutionTags());

        Assertions.assertEquals(WorkflowStatus.IN_PROGRESS, info.getWorkflowStatus());

        SwfClient remoteSwf = createSwfClient(false);
        terminateOpenWorkflowExecutions(remoteSwf);
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
