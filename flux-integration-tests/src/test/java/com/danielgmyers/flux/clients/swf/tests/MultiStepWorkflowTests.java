package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Tests that validate Flux's behavior for multi-step workflows.
 */
public class MultiStepWorkflowTests extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(MultiStepWorkflowTests.class);

    private static final Map<String, List<String>> EXECUTION_ORDER_BY_WORKFLOW_ID = Collections.synchronizedMap(new HashMap<>());

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new MultiStep());
    }

    int getWorkerPoolThreadCount() {
        return 30; // we want the workflows to run as fast as reasonably possible
    }

    @Test
    public void testMultiStepWorkflowRunsStepsInExpectedOrder() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));

        executeWorkflow(MultiStep.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

        Assertions.assertEquals(3, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assertions.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
        Assertions.assertEquals(StepThree.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(2));
    }

    @Test
    public void testMultiStepWorkflowRunsStepsInExpectedOrderWithHighParallelism() throws InterruptedException {
        Set<String> uuids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));
            executeWorkflow(MultiStep.class, uuid, Collections.emptyMap());
        }

        for (String uuid : uuids) {
            WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

            Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

            Assertions.assertEquals(3, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
            Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
            Assertions.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
            Assertions.assertEquals(StepThree.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(2));
        }
    }

    /**
     * Workflow with three steps.
     */
    public static final class MultiStep implements Workflow {
        private final WorkflowGraph graph;

        MultiStep() {
            WorkflowStep stepOne = new StepOne();
            WorkflowStep stepTwo = new StepTwo();
            WorkflowStep stepThree = new StepThree();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.alwaysTransition(stepOne, stepTwo);

            builder.addStep(stepTwo);
            builder.alwaysTransition(stepTwo, stepThree);

            builder.addStep(stepThree);
            builder.alwaysClose(stepThree);

            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Base step implementation that adds its step name to the EXECUTION_ORDER_BY_WORKFLOW_ID list for the relevant workflow id.
     */
    public abstract static class StepBase implements WorkflowStep {
        /**
         * Does the thing.
         */
        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            if (EXECUTION_ORDER_BY_WORKFLOW_ID.containsKey(workflowId)) {
                EXECUTION_ORDER_BY_WORKFLOW_ID.get(workflowId).add(this.getClass().getSimpleName());
            } else {
                log.warn("Received execution for unrecognized workflow id " + workflowId + ", ignoring.");
            }
        }
    }

    /**
     * Implementation of the base named StepOne.
     */
    public static final class StepOne extends StepBase {}

    /**
     * Implementation of the base named StepTwo.
     */
    public static final class StepTwo extends StepBase {}

    /**
     * Implementation of the base named StepThree.
     */
    public static final class StepThree extends StepBase {}
}
