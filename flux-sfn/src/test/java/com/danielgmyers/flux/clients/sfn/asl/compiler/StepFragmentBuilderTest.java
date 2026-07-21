/*
 *   Copyright Flux Contributors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.danielgmyers.flux.clients.sfn.asl.compiler;

import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceRule;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceState;
import com.danielgmyers.flux.clients.sfn.asl.state.FailState;
import com.danielgmyers.flux.clients.sfn.asl.state.MapState;
import com.danielgmyers.flux.clients.sfn.asl.state.Retrier;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StepFragmentBuilderTest {

    private static final String REGION = "us-west-2";
    private static final String ACCOUNT_ID = "123456789012";

    private FluxCapacitorConfig config;
    private StepFragmentBuilder builder;

    @BeforeEach
    public void setup() {
        config = new FluxCapacitorConfig();
        config.setAwsRegion(REGION);
        config.setAwsAccountId(ACCOUNT_ID);
        builder = new StepFragmentBuilder(TestWorkflow.class, config);
    }

    @Test
    public void testAlwaysTransitionStep() {
        WorkflowGraph graph = buildGraphWithAlwaysTransition();
        WorkflowGraphNode node = graph.getNodes().get(AlwaysStep.class);

        StepFragment fragment = builder.build(node);

        // Entry is the Task state
        Assertions.assertEquals("TestWorkflow.AlwaysStep", fragment.getEntryStateName());

        // Only one state: the Task
        Assertions.assertEquals(1, fragment.getStates().size());
        AslState state = fragment.getStates().get("TestWorkflow.AlwaysStep");
        Assertions.assertInstanceOf(TaskState.class, state);

        TaskState taskState = (TaskState) state;
        String expectedArn = SfnArnFormatter.activityArn(REGION, ACCOUNT_ID, TestWorkflow.class, AlwaysStep.class);
        Assertions.assertEquals(expectedArn, taskState.getResource());
        Assertions.assertNotNull(taskState.getRetry());
        Assertions.assertNull(taskState.getNext()); // Next resolved by assembler

        // Exit transitions: single _always entry pointing to the task state name
        Assertions.assertEquals(1, fragment.getExitTransitionResultCodes().size());
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.ALWAYS_RESULT_CODE));
        Assertions.assertEquals("TestWorkflow.AlwaysStep",
                                fragment.getExitTransitionResultCodes().get(StepResult.ALWAYS_RESULT_CODE));
    }

    @Test
    public void testBranchingStep() {
        WorkflowGraph graph = buildGraphWithBranching();
        WorkflowGraphNode node = graph.getNodes().get(BranchingStep.class);

        StepFragment fragment = builder.build(node);

        // Entry is the Task state
        Assertions.assertEquals("TestWorkflow.BranchingStep", fragment.getEntryStateName());

        // Three states: Task, Choice, Fail
        Assertions.assertEquals(3, fragment.getStates().size());

        // Task state
        AslState taskState = fragment.getStates().get("TestWorkflow.BranchingStep");
        Assertions.assertInstanceOf(TaskState.class, taskState);
        Assertions.assertEquals("TestWorkflow.BranchingStep.Route", ((TaskState) taskState).getNext());

        // Choice state
        AslState choiceState = fragment.getStates().get("TestWorkflow.BranchingStep.Route");
        Assertions.assertInstanceOf(ChoiceState.class, choiceState);
        ChoiceState choice = (ChoiceState) choiceState;
        Assertions.assertEquals("TestWorkflow.BranchingStep.BadResult", choice.getDefaultState());
        Assertions.assertNotNull(choice.getChoices());
        Assertions.assertEquals(2, choice.getChoices().size());

        // Verify choice rules check the result code path
        for (ChoiceRule rule : choice.getChoices()) {
            Assertions.assertEquals(StepFragmentBuilder.RESULT_CODE_JSON_PATH, rule.getVariable());
        }

        // Fail state
        AslState failState = fragment.getStates().get("TestWorkflow.BranchingStep.BadResult");
        Assertions.assertInstanceOf(FailState.class, failState);
        Assertions.assertEquals("Flux.UnrecognizedResultCode", ((FailState) failState).getError());

        // Exit transitions: one per result code, all pointing to the route state
        Assertions.assertEquals(2, fragment.getExitTransitionResultCodes().size());
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.FAIL_RESULT_CODE));
    }

    @Test
    public void testSingleNonAlwaysTransitionStillProducesChoice() {
        WorkflowGraph graph = buildGraphWithSingleSuccessTransition();
        WorkflowGraphNode node = graph.getNodes().get(SuccessOnlyStep.class);

        StepFragment fragment = builder.build(node);

        // Should still have Task + Choice + Fail (only _always elides the Choice)
        Assertions.assertEquals(3, fragment.getStates().size());
        Assertions.assertInstanceOf(TaskState.class, fragment.getStates().get("TestWorkflow.SuccessOnlyStep"));
        Assertions.assertInstanceOf(ChoiceState.class, fragment.getStates().get("TestWorkflow.SuccessOnlyStep.Route"));
        Assertions.assertInstanceOf(FailState.class, fragment.getStates().get("TestWorkflow.SuccessOnlyStep.BadResult"));
    }

    @Test
    public void testRetryConfigDefaultValues() {
        WorkflowGraph graph = buildGraphWithAlwaysTransition();
        WorkflowGraphNode node = graph.getNodes().get(AlwaysStep.class);

        List<Retrier> retriers = builder.buildRetryConfig(node.getStep());

        // Default @StepApply: initialRetryDelaySeconds=10, retriesBeforeBackoff=6,
        // maxRetryDelaySeconds=600, jitterPercent=10, exponentialBackoffBase=0 (uses config default)
        Assertions.assertEquals(2, retriers.size());

        // Retrier 1: flat-rate
        Retrier flat = retriers.get(0);
        Assertions.assertEquals(List.of("States.TaskFailed"), flat.getErrorEquals());
        Assertions.assertEquals(10, flat.getIntervalSeconds());
        Assertions.assertEquals(6, flat.getMaxAttempts());
        Assertions.assertEquals(1.0, flat.getBackoffRate());
        Assertions.assertEquals("FULL", flat.getJitterStrategy());

        // Retrier 2: exponential backoff
        Retrier backoff = retriers.get(1);
        Assertions.assertEquals(List.of("States.TaskFailed"), backoff.getErrorEquals());
        Assertions.assertEquals(10, backoff.getIntervalSeconds());
        Assertions.assertEquals(4000, backoff.getMaxAttempts());
        Assertions.assertEquals(1.25, backoff.getBackoffRate()); // default base
        Assertions.assertEquals(600, backoff.getMaxDelaySeconds());
        Assertions.assertEquals("FULL", backoff.getJitterStrategy());
    }

    @Test
    public void testRetryConfigCustomValues() {
        WorkflowGraph graph = buildGraphWithCustomRetryStep();
        WorkflowGraphNode node = graph.getNodes().get(CustomRetryStep.class);

        List<Retrier> retriers = builder.buildRetryConfig(node.getStep());

        Assertions.assertEquals(2, retriers.size());

        // Retrier 1: flat with custom initial delay and retries before backoff
        Retrier flat = retriers.get(0);
        Assertions.assertEquals(5, flat.getIntervalSeconds());
        Assertions.assertEquals(3, flat.getMaxAttempts());
        Assertions.assertEquals(1.0, flat.getBackoffRate());
        Assertions.assertNull(flat.getJitterStrategy()); // jitterPercent=0

        // Retrier 2: exponential backoff with step-level override
        Retrier backoff = retriers.get(1);
        Assertions.assertEquals(5, backoff.getIntervalSeconds());
        Assertions.assertEquals(4000, backoff.getMaxAttempts());
        Assertions.assertEquals(3.0, backoff.getBackoffRate()); // step override
        Assertions.assertEquals(120, backoff.getMaxDelaySeconds());
        Assertions.assertNull(backoff.getJitterStrategy()); // jitterPercent=0
    }

    @Test
    public void testRetryConfigUsesFluxCapacitorConfigBackoffBase() {
        config.setExponentialBackoffBase(2.5);
        builder = new StepFragmentBuilder(TestWorkflow.class, config);

        WorkflowGraph graph = buildGraphWithAlwaysTransition();
        WorkflowGraphNode node = graph.getNodes().get(AlwaysStep.class);

        List<Retrier> retriers = builder.buildRetryConfig(node.getStep());

        // Should use the config-provided base since the step doesn't override it
        Retrier backoff = retriers.get(1);
        Assertions.assertEquals(2.5, backoff.getBackoffRate());
    }

    @Test
    public void testRetryConfigOmitsFlatRetrierWhenRetriesBeforeBackoffIsOne() {
        List<Retrier> retriers = builder.buildRetryConfig(new NoFlatRetrierStep());

        // Only the backoff retrier should be present
        Assertions.assertEquals(1, retriers.size());

        Retrier backoff = retriers.get(0);
        Assertions.assertEquals(List.of("States.TaskFailed"), backoff.getErrorEquals());
        Assertions.assertEquals(10, backoff.getIntervalSeconds());
        Assertions.assertEquals(4000, backoff.getMaxAttempts());
        Assertions.assertEquals(300, backoff.getMaxDelaySeconds());
        Assertions.assertTrue(backoff.getBackoffRate() > 1.0);
    }

    @Test
    public void testRetryConfigOmitsBackoffRetrierWhenMaxEqualsInitial() {
        List<Retrier> retriers = builder.buildRetryConfig(new NoBackoffRetrierStep());

        // Only the flat retrier should be present (with MAX_RETRY_ATTEMPTS since it's the sole retrier)
        Assertions.assertEquals(1, retriers.size());

        Retrier flat = retriers.get(0);
        Assertions.assertEquals(List.of("States.TaskFailed"), flat.getErrorEquals());
        Assertions.assertEquals(10, flat.getIntervalSeconds());
        Assertions.assertEquals(4000, flat.getMaxAttempts());
        Assertions.assertEquals(1.0, flat.getBackoffRate());
    }

    @Test
    public void testRetryConfigFallsBackToSingleFlatRetrierWhenBothOmitted() {
        List<Retrier> retriers = builder.buildRetryConfig(new NeitherRetrierStep());

        // Both conditions for omission met, so we get the fallback flat retrier
        Assertions.assertEquals(1, retriers.size());

        Retrier flat = retriers.get(0);
        Assertions.assertEquals(List.of("States.TaskFailed"), flat.getErrorEquals());
        Assertions.assertEquals(10, flat.getIntervalSeconds());
        Assertions.assertEquals(4000, flat.getMaxAttempts());
        Assertions.assertEquals(1.0, flat.getBackoffRate());
        Assertions.assertNull(flat.getMaxDelaySeconds());
    }

    @Test
    public void testPartitionedStepWithAlwaysTransition() {
        WorkflowGraph graph = buildGraphWithPartitionedAlwaysStep();
        WorkflowGraphNode node = graph.getNodes().get(PartitionedAlwaysStep.class);

        StepFragment fragment = builder.build(node);

        // Entry is the GenParts Task
        Assertions.assertEquals("TestWorkflow.PartitionedAlwaysStep.GenParts", fragment.getEntryStateName());

        // Two states: GenParts Task and MapParts Map
        Assertions.assertEquals(2, fragment.getStates().size());

        // GenParts Task
        AslState genParts = fragment.getStates().get("TestWorkflow.PartitionedAlwaysStep.GenParts");
        Assertions.assertInstanceOf(TaskState.class, genParts);
        TaskState genPartsTask = (TaskState) genParts;
        Assertions.assertEquals("TestWorkflow.PartitionedAlwaysStep.MapParts", genPartsTask.getNext());
        String expectedGenPartsArn = SfnArnFormatter.activityArn(REGION, ACCOUNT_ID, TestWorkflow.class,
                                                                  PartitionedAlwaysStep.class,
                                                                  StepFragment.SUFFIX_GENERATE_PARTITIONS);
        Assertions.assertEquals(expectedGenPartsArn, genPartsTask.getResource());

        // MapParts Map
        AslState mapParts = fragment.getStates().get("TestWorkflow.PartitionedAlwaysStep.MapParts");
        Assertions.assertInstanceOf(MapState.class, mapParts);
        MapState mapState = (MapState) mapParts;
        Assertions.assertEquals("$._flux_partitionIds", mapState.getItemsPath());
        Assertions.assertEquals(0, mapState.getMaxConcurrency());
        Assertions.assertNull(mapState.getNext()); // _always, resolved by assembler

        // ItemProcessor
        Assertions.assertNotNull(mapState.getItemProcessor());
        Assertions.assertEquals("TestWorkflow.PartitionedAlwaysStep.Partition",
                                mapState.getItemProcessor().getStartAt());
        AslState innerTask = mapState.getItemProcessor().getStates()
                .get("TestWorkflow.PartitionedAlwaysStep.Partition");
        Assertions.assertInstanceOf(TaskState.class, innerTask);
        Assertions.assertEquals(true, innerTask.getEnd());
        String expectedPartitionArn = SfnArnFormatter.activityArn(REGION, ACCOUNT_ID, TestWorkflow.class,
                                                                   PartitionedAlwaysStep.class);
        Assertions.assertEquals(expectedPartitionArn, ((TaskState) innerTask).getResource());

        // Exit transitions
        Assertions.assertEquals(1, fragment.getExitTransitionResultCodes().size());
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.ALWAYS_RESULT_CODE));
    }

    @Test
    public void testPartitionedStepWithBranching() {
        WorkflowGraph graph = buildGraphWithPartitionedBranchingStep();
        WorkflowGraphNode node = graph.getNodes().get(PartitionedBranchingStep.class);

        StepFragment fragment = builder.build(node);

        // Entry is the GenParts Task
        Assertions.assertEquals("TestWorkflow.PartitionedBranchingStep.GenParts", fragment.getEntryStateName());

        // Four states: GenParts, MapParts, Route, BadResult
        Assertions.assertEquals(4, fragment.getStates().size());
        Assertions.assertInstanceOf(TaskState.class,
                fragment.getStates().get("TestWorkflow.PartitionedBranchingStep.GenParts"));
        Assertions.assertInstanceOf(MapState.class,
                fragment.getStates().get("TestWorkflow.PartitionedBranchingStep.MapParts"));
        Assertions.assertInstanceOf(ChoiceState.class,
                fragment.getStates().get("TestWorkflow.PartitionedBranchingStep.Route"));
        Assertions.assertInstanceOf(FailState.class,
                fragment.getStates().get("TestWorkflow.PartitionedBranchingStep.BadResult"));

        // Map state should point to Route
        MapState mapState = (MapState) fragment.getStates().get("TestWorkflow.PartitionedBranchingStep.MapParts");
        Assertions.assertEquals("TestWorkflow.PartitionedBranchingStep.Route", mapState.getNext());

        // Exit transitions
        Assertions.assertEquals(2, fragment.getExitTransitionResultCodes().size());
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertTrue(fragment.getExitTransitionResultCodes().containsKey(StepResult.FAIL_RESULT_CODE));
    }

    @Test
    public void testFormatStateName() {
        Assertions.assertEquals("TestWorkflow.MyStep", builder.formatStateName("MyStep", null));
        Assertions.assertEquals("TestWorkflow.MyStep.Route", builder.formatStateName("MyStep", "Route"));
        Assertions.assertEquals("TestWorkflow.MyStep.BadResult", builder.formatStateName("MyStep", "BadResult"));
    }

    // --- Test step/workflow classes ---

    public static class TestWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            return null;
        }
    }

    public static class AlwaysStep implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class SecondStep implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class BranchingStep implements WorkflowStep {
        @StepApply
        public StepResult doWork() {
            return StepResult.success();
        }
    }

    public static class FailureHandler implements WorkflowStep {
        @StepApply
        public void handle() {
        }
    }

    public static class SuccessOnlyStep implements WorkflowStep {
        @StepApply
        public StepResult doWork() {
            return StepResult.success();
        }
    }

    public static class CustomRetryStep implements WorkflowStep {
        @StepApply(initialRetryDelaySeconds = 5, maxRetryDelaySeconds = 120,
                   retriesBeforeBackoff = 3, jitterPercent = 0, exponentialBackoffBase = 3.0)
        public void doWork() {
        }
    }

    public static class NoFlatRetrierStep implements WorkflowStep {
        @StepApply(initialRetryDelaySeconds = 10, maxRetryDelaySeconds = 300, retriesBeforeBackoff = 1)
        public void doWork() {
        }
    }

    public static class NoBackoffRetrierStep implements WorkflowStep {
        @StepApply(initialRetryDelaySeconds = 10, maxRetryDelaySeconds = 10, retriesBeforeBackoff = 5)
        public void doWork() {
        }
    }

    public static class NeitherRetrierStep implements WorkflowStep {
        @StepApply(initialRetryDelaySeconds = 10, maxRetryDelaySeconds = 10, retriesBeforeBackoff = 1)
        public void doWork() {
        }
    }

    public static class PartitionedAlwaysStep implements PartitionedWorkflowStep {
        @PartitionIdGenerator
        public PartitionIdGeneratorResult getPartitions() {
            return PartitionIdGeneratorResult.create();
        }

        @StepApply
        public void doWork(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
        }
    }

    public static class PartitionedBranchingStep implements PartitionedWorkflowStep {
        @PartitionIdGenerator
        public PartitionIdGeneratorResult getPartitions() {
            return PartitionIdGeneratorResult.create();
        }

        @StepApply
        public StepResult doWork(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
            return StepResult.success();
        }
    }

    // --- Graph construction helpers ---

    private WorkflowGraph buildGraphWithAlwaysTransition() {
        AlwaysStep alwaysStep = new AlwaysStep();
        SecondStep secondStep = new SecondStep();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(alwaysStep);
        graphBuilder.alwaysTransition(alwaysStep, secondStep);
        graphBuilder.addStep(secondStep);
        graphBuilder.alwaysClose(secondStep);
        return graphBuilder.build();
    }

    private WorkflowGraph buildGraphWithBranching() {
        BranchingStep branchingStep = new BranchingStep();
        SecondStep secondStep = new SecondStep();
        FailureHandler failureHandler = new FailureHandler();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(branchingStep);
        graphBuilder.commonTransitions(branchingStep, secondStep, failureHandler);
        graphBuilder.addStep(secondStep);
        graphBuilder.alwaysClose(secondStep);
        graphBuilder.addStep(failureHandler);
        graphBuilder.alwaysClose(failureHandler);
        return graphBuilder.build();
    }

    private WorkflowGraph buildGraphWithSingleSuccessTransition() {
        SuccessOnlyStep successOnlyStep = new SuccessOnlyStep();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(successOnlyStep);
        graphBuilder.closeOnSuccess(successOnlyStep);
        return graphBuilder.build();
    }

    private WorkflowGraph buildGraphWithCustomRetryStep() {
        CustomRetryStep customRetryStep = new CustomRetryStep();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(customRetryStep);
        graphBuilder.alwaysClose(customRetryStep);
        return graphBuilder.build();
    }

    private WorkflowGraph buildGraphWithPartitionedAlwaysStep() {
        PartitionedAlwaysStep partitionedStep = new PartitionedAlwaysStep();
        SecondStep secondStep = new SecondStep();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(partitionedStep);
        graphBuilder.alwaysTransition(partitionedStep, secondStep);
        graphBuilder.addStep(secondStep);
        graphBuilder.alwaysClose(secondStep);
        return graphBuilder.build();
    }

    private WorkflowGraph buildGraphWithPartitionedBranchingStep() {
        PartitionedBranchingStep partitionedStep = new PartitionedBranchingStep();
        SecondStep secondStep = new SecondStep();
        FailureHandler failureHandler = new FailureHandler();
        WorkflowGraphBuilder graphBuilder = new WorkflowGraphBuilder(partitionedStep);
        graphBuilder.commonTransitions(partitionedStep, secondStep, failureHandler);
        graphBuilder.addStep(secondStep);
        graphBuilder.alwaysClose(secondStep);
        graphBuilder.addStep(failureHandler);
        graphBuilder.alwaysClose(failureHandler);
        return graphBuilder.build();
    }
}
