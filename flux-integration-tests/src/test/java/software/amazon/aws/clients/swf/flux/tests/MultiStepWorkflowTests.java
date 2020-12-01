package software.amazon.aws.clients.swf.flux.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

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
        waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assert.assertEquals(3, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assert.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assert.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
        Assert.assertEquals(StepThree.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(2));
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
            waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

            Assert.assertEquals(3, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
            Assert.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
            Assert.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
            Assert.assertEquals(StepThree.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(2));
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
