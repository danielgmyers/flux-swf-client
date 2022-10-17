package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Given an SwfClient object, there's not actually a way to check its configuration.
 * In order to verify that FluxCapacitorConfig's ClientOverrideConfiguration field
 * got wired into the SwfClient, we'll configure it with an execution interceptor
 * and verify that the interceptor got called during the HelloWorld workflow.
 */
public class CustomSdkConfigurationTest extends WorkflowTestBase {

    private boolean interceptorCalled;

    ExecutionInterceptor helloInterceptor = new ExecutionInterceptor() {
        @Override
        public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes) {
            interceptorCalled = true;
        }
    };

    @Override
    protected void updateFluxCapacitorConfig(FluxCapacitorConfig config) {
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(helloInterceptor)
                .build();
        config.setClientOverrideConfiguration(overrideConfig);
    }

    @Before
    public void clearInterceptorCalled() {
        interceptorCalled = false;
    }

    @After
    public void verifyInterceptorCalled() {
        Assert.assertTrue("If this fails, then FluxCapacitorConfig's ClientOverrideConfiguration was not wired properly.",
                          interceptorCalled);
    }

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new HelloWorld());
    }

    /**
     * Tests that a single-step workflow is executed the expected number of times by worker threads.
     */
    @Test
    public void testBasicWorkflow() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        executeWorkflow(HelloWorld.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(15));

        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assert.assertTrue(StepOne.didExecute(uuid));

        uuid = UUID.randomUUID().toString();
        executeWorkflow(HelloWorld.class, uuid, Collections.emptyMap());
        info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(15));

        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assert.assertTrue(StepOne.didExecute(uuid));
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
        private final static Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
