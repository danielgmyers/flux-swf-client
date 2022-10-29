package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Tests that validate Flux's behavior for branching workflows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class BranchingWorkflowTests extends WorkflowTestBase {

    private static final String BRANCH_ATTRIBUTE_NAME = "branch";
    private static final String BRANCH_LEFT = "left";
    private static final String BRANCH_RIGHT = "right";

    private static final Logger log = LoggerFactory.getLogger(BranchingWorkflowTests.class);
    private static final Map<String, List<String>> EXECUTION_ORDER_BY_WORKFLOW_ID = Collections.synchronizedMap(new HashMap<>());

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Arrays.asList(new BranchingWorkflowSucceedFail(), new BranchingWorkflowCustomCodes());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void testBranchingWorkflowTakesExpectedBranch_SucceedFailBranches() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));

        executeWorkflow(BranchingWorkflowSucceedFail.class, uuid,

                        buildInput(BRANCH_ATTRIBUTE_NAME, StepResult.SUCCEED_RESULT_CODE));
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

        Assertions.assertEquals(2, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assertions.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));

        uuid = UUID.randomUUID().toString();
        EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));

        executeWorkflow(BranchingWorkflowSucceedFail.class, uuid,
                        buildInput(BRANCH_ATTRIBUTE_NAME, StepResult.FAIL_RESULT_CODE));
        info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assertions.assertEquals(2, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assertions.assertEquals(StepThree.class.getSimpleName(),EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
    }

    @Test
    public void testBranchingWorkflowTakesExpectedBranch_CustomCodes() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));

        executeWorkflow(BranchingWorkflowCustomCodes.class, uuid, buildInput(BRANCH_ATTRIBUTE_NAME, BRANCH_LEFT));
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assertions.assertEquals(2, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assertions.assertEquals(StepTwo.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));

        uuid = UUID.randomUUID().toString();
        EXECUTION_ORDER_BY_WORKFLOW_ID.put(uuid, Collections.synchronizedList(new LinkedList<>()));

        executeWorkflow(BranchingWorkflowCustomCodes.class, uuid, buildInput(BRANCH_ATTRIBUTE_NAME, BRANCH_RIGHT));
        info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Assertions.assertEquals(2, EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).size());
        Assertions.assertEquals(StepOne.class.getSimpleName(), EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(0));
        Assertions.assertEquals(StepThree.class.getSimpleName(),EXECUTION_ORDER_BY_WORKFLOW_ID.get(uuid).get(1));
    }

    private static <T> Map<String, T> buildInput(String key, T value) {
        Map<String, T> input = new HashMap<>();
        input.put(key, value);
        return input;
    }

    /**
     * Workflow with an initial step and two branches.
     */
    public static final class BranchingWorkflowSucceedFail implements Workflow {
        private final WorkflowGraph graph;

        BranchingWorkflowSucceedFail() {
            WorkflowStep stepOne = new StepOne();
            WorkflowStep stepTwo = new StepTwo();
            WorkflowStep stepThree = new StepThree();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, buildInput(BRANCH_ATTRIBUTE_NAME, String.class));
            builder.successTransition(stepOne, stepTwo);
            builder.failTransition(stepOne, stepThree);

            builder.addStep(stepTwo);
            builder.alwaysClose(stepTwo);

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
     * Workflow with an initial step and two branches.
     */
    public static final class BranchingWorkflowCustomCodes implements Workflow {
        private final WorkflowGraph graph;

        BranchingWorkflowCustomCodes() {
            WorkflowStep stepOne = new StepOne();
            WorkflowStep stepTwo = new StepTwo();
            WorkflowStep stepThree = new StepThree();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, buildInput(BRANCH_ATTRIBUTE_NAME, String.class));
            builder.customTransition(stepOne, BRANCH_LEFT, stepTwo);
            builder.customTransition(stepOne, BRANCH_RIGHT, stepThree);

            builder.addStep(stepTwo);
            builder.alwaysClose(stepTwo);

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
     * Initial step that branches left or right based on the step input.
     */
    public static final class StepOne implements WorkflowStep {
        /**
         * Does the thing.
         */
        @StepApply
        public StepResult doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                  @Attribute(BRANCH_ATTRIBUTE_NAME) String branch) {
            if (EXECUTION_ORDER_BY_WORKFLOW_ID.containsKey(workflowId)) {
                EXECUTION_ORDER_BY_WORKFLOW_ID.get(workflowId).add(this.getClass().getSimpleName());
            } else {
                log.warn("Received execution for unrecognized workflow id " + workflowId + ", ignoring.");
            }
            return StepResult.complete(branch, "Returning specified branch: " + branch);
        }
    }

    /**
     * Step that just records which workflows it has executed in.
     */
    public static final class StepTwo implements WorkflowStep {
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
     * Another step that just records which workflows it has executed in.
     */
    public static final class StepThree implements WorkflowStep {
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
}
