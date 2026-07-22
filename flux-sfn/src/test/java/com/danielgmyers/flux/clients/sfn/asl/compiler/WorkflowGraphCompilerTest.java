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

import java.util.Map;

import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.asl.StateMachineDefinition;
import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceRule;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceState;
import com.danielgmyers.flux.clients.sfn.asl.state.FailState;
import com.danielgmyers.flux.clients.sfn.asl.state.MapState;
import com.danielgmyers.flux.clients.sfn.asl.state.SucceedState;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkflowGraphCompilerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FluxCapacitorConfig config;
    private WorkflowGraphCompiler compiler;

    @BeforeEach
    public void setup() {
        config = new FluxCapacitorConfig();
        config.setAwsRegion("us-west-2");
        config.setAwsAccountId("123456789012");
        compiler = new WorkflowGraphCompiler(config);
    }

    @Test
    public void testSimpleTwoStepWorkflow() {
        StateMachineDefinition def = compiler.compile(new TwoStepWorkflow());

        Assertions.assertEquals("TwoStepWorkflow", def.getComment());
        Assertions.assertEquals("TwoStepWorkflow.StepOne", def.getStartAt());
        Assertions.assertNotNull(def.getStates());

        // StepOne (always) → StepTwo (always) → Succeed
        // States: StepOne Task, StepTwo Task, Succeed, Failed
        Assertions.assertEquals(4, def.getStates().size());
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("TwoStepWorkflow.StepOne"));
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("TwoStepWorkflow.StepTwo"));
        Assertions.assertInstanceOf(SucceedState.class, def.getStates().get("TwoStepWorkflow.Succeed"));

        // Verify transitions are wired
        TaskState stepOne = (TaskState) def.getStates().get("TwoStepWorkflow.StepOne");
        Assertions.assertEquals("TwoStepWorkflow.StepTwo", stepOne.getNext());

        TaskState stepTwo = (TaskState) def.getStates().get("TwoStepWorkflow.StepTwo");
        Assertions.assertEquals("TwoStepWorkflow.Succeed", stepTwo.getNext());
    }

    @Test
    public void testBranchingWorkflow() {
        StateMachineDefinition def = compiler.compile(new BranchingWorkflow());

        Assertions.assertEquals("BranchingWorkflow", def.getComment());
        Assertions.assertEquals("BranchingWorkflow.DecisionStep", def.getStartAt());

        // DecisionStep (Task + Choice + Fail), SuccessPath (Task), FailurePath (Task), Succeed, Failed
        Assertions.assertEquals(7, def.getStates().size());
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("BranchingWorkflow.DecisionStep"));
        Assertions.assertInstanceOf(ChoiceState.class, def.getStates().get("BranchingWorkflow.DecisionStep.Route"));
        Assertions.assertInstanceOf(FailState.class, def.getStates().get("BranchingWorkflow.DecisionStep.BadResult"));
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("BranchingWorkflow.SuccessPath"));
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("BranchingWorkflow.FailurePath"));
        Assertions.assertInstanceOf(SucceedState.class, def.getStates().get("BranchingWorkflow.Succeed"));

        // Verify choice rules point to the right targets
        ChoiceState choice = (ChoiceState) def.getStates().get("BranchingWorkflow.DecisionStep.Route");
        Map<String, String> ruleTargets = new java.util.HashMap<>();
        for (ChoiceRule rule : choice.getChoices()) {
            ruleTargets.put(rule.getStringEquals(), rule.getNext());
        }
        Assertions.assertEquals("BranchingWorkflow.SuccessPath", ruleTargets.get(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertEquals("BranchingWorkflow.FailurePath", ruleTargets.get(StepResult.FAIL_RESULT_CODE));

        // SuccessPath and FailurePath both go to Succeed
        TaskState successPath = (TaskState) def.getStates().get("BranchingWorkflow.SuccessPath");
        Assertions.assertEquals("BranchingWorkflow.Succeed", successPath.getNext());

        TaskState failurePath = (TaskState) def.getStates().get("BranchingWorkflow.FailurePath");
        Assertions.assertEquals("BranchingWorkflow.Succeed", failurePath.getNext());
    }

    @Test
    public void testCloseWorkflowTransitionPointsToSucceed() {
        StateMachineDefinition def = compiler.compile(new SingleStepCloseWorkflow());

        Assertions.assertEquals("SingleStepCloseWorkflow", def.getComment());
        Assertions.assertEquals("SingleStepCloseWorkflow.OnlyStep", def.getStartAt());

        // OnlyStep (Task + Choice + Fail) + Succeed + Failed
        Assertions.assertEquals(5, def.getStates().size());

        // The success choice rule should point to Succeed
        ChoiceState choice = (ChoiceState) def.getStates().get("SingleStepCloseWorkflow.OnlyStep.Route");
        ChoiceRule successRule = choice.getChoices().stream()
                .filter(r -> StepResult.SUCCEED_RESULT_CODE.equals(r.getStringEquals()))
                .findFirst().orElse(null);
        Assertions.assertNotNull(successRule);
        Assertions.assertEquals("SingleStepCloseWorkflow.Succeed", successRule.getNext());
    }

    @Test
    public void testFailResultCodeClosesWorkflowWithFailedState() {
        StateMachineDefinition def = compiler.compile(new SucceedOrFailWorkflow());

        // The _succeed rule should point to Succeed, the _fail rule should point to Failed
        ChoiceState choice = (ChoiceState) def.getStates().get("SucceedOrFailWorkflow.MayFailStep.Route");
        Assertions.assertNotNull(choice);

        Map<String, String> ruleTargets = new java.util.HashMap<>();
        for (ChoiceRule rule : choice.getChoices()) {
            ruleTargets.put(rule.getStringEquals(), rule.getNext());
        }

        Assertions.assertEquals("SucceedOrFailWorkflow.Succeed", ruleTargets.get(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertEquals("SucceedOrFailWorkflow.Failed", ruleTargets.get(StepResult.FAIL_RESULT_CODE));

        // Verify the Failed state exists and is a Fail type
        AslState failedState = def.getStates().get("SucceedOrFailWorkflow.Failed");
        Assertions.assertInstanceOf(com.danielgmyers.flux.clients.sfn.asl.state.FailState.class, failedState);
    }

    @Test
    public void testPartitionedStepWorkflow() {
        StateMachineDefinition def = compiler.compile(new PartitionedWorkflow());

        Assertions.assertEquals("PartitionedWorkflow", def.getComment());
        Assertions.assertEquals("PartitionedWorkflow.PartStep.GenParts", def.getStartAt());

        // PartStep: GenParts, MapParts, Succeed
        Assertions.assertInstanceOf(TaskState.class, def.getStates().get("PartitionedWorkflow.PartStep.GenParts"));
        Assertions.assertInstanceOf(MapState.class, def.getStates().get("PartitionedWorkflow.PartStep.MapParts"));
        Assertions.assertInstanceOf(SucceedState.class, def.getStates().get("PartitionedWorkflow.Succeed"));

        // MapParts Next should point to Succeed (since it's _always)
        MapState mapState = (MapState) def.getStates().get("PartitionedWorkflow.PartStep.MapParts");
        Assertions.assertEquals("PartitionedWorkflow.Succeed", mapState.getNext());
    }

    @Test
    public void testOutputIsValidJson() throws JsonProcessingException {
        StateMachineDefinition def = compiler.compile(new TwoStepWorkflow());

        String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(def);
        Assertions.assertNotNull(json);
        Assertions.assertTrue(json.contains("\"StartAt\""));
        Assertions.assertTrue(json.contains("\"States\""));
        Assertions.assertTrue(json.contains("\"TwoStepWorkflow.StepOne\""));
        Assertions.assertTrue(json.contains("\"Type\" : \"Task\""));
        Assertions.assertTrue(json.contains("\"Type\" : \"Succeed\""));

        // Verify it can be deserialized back
        StateMachineDefinition roundTripped = MAPPER.readValue(json, StateMachineDefinition.class);
        Assertions.assertEquals(def.getStartAt(), roundTripped.getStartAt());
        Assertions.assertEquals(def.getStates().size(), roundTripped.getStates().size());
    }

    @Test
    public void testMultiStepBranchingWorkflow() {
        StateMachineDefinition def = compiler.compile(new MultiStepBranchWorkflow());

        // StepA branches to StepB (success) or StepC (failure), both close
        // StepA: Task + Choice + Fail = 3 states
        // StepB: Task = 1 state (always close)
        // StepC: Task = 1 state (always close)
        // Succeed + Failed = 2 states
        // Total = 7
        Assertions.assertEquals(7, def.getStates().size());

        ChoiceState choice = (ChoiceState) def.getStates().get("MultiStepBranchWorkflow.StepA.Route");
        Map<String, String> ruleTargets = new java.util.HashMap<>();
        for (ChoiceRule rule : choice.getChoices()) {
            ruleTargets.put(rule.getStringEquals(), rule.getNext());
        }
        Assertions.assertEquals("MultiStepBranchWorkflow.StepB", ruleTargets.get(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertEquals("MultiStepBranchWorkflow.StepC", ruleTargets.get(StepResult.FAIL_RESULT_CODE));

        // Both StepB and StepC should go to Succeed
        TaskState stepB = (TaskState) def.getStates().get("MultiStepBranchWorkflow.StepB");
        Assertions.assertEquals("MultiStepBranchWorkflow.Succeed", stepB.getNext());

        TaskState stepC = (TaskState) def.getStates().get("MultiStepBranchWorkflow.StepC");
        Assertions.assertEquals("MultiStepBranchWorkflow.Succeed", stepC.getNext());
    }

    // --- Test workflow classes ---

    public static class StepOne implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class StepTwo implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class DecisionStep implements WorkflowStep {
        @StepApply
        public StepResult decide() {
            return StepResult.success();
        }
    }

    public static class SuccessPath implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class FailurePath implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class OnlyStep implements WorkflowStep {
        @StepApply
        public StepResult doWork() {
            return StepResult.success();
        }
    }

    public static class StepA implements WorkflowStep {
        @StepApply
        public StepResult doWork() {
            return StepResult.success();
        }
    }

    public static class MayFailStep implements WorkflowStep {
        @StepApply
        public StepResult doWork() {
            return StepResult.success();
        }
    }

    public static class StepB implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class StepC implements WorkflowStep {
        @StepApply
        public void doWork() {
        }
    }

    public static class PartStep implements PartitionedWorkflowStep {
        @PartitionIdGenerator
        public PartitionIdGeneratorResult getPartitions() {
            return PartitionIdGeneratorResult.create();
        }

        @StepApply
        public void doWork(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
        }
    }

    // --- Test workflow definitions ---

    public static class TwoStepWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            StepOne stepOne = new StepOne();
            StepTwo stepTwo = new StepTwo();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne);
            builder.alwaysTransition(stepOne, stepTwo);
            builder.addStep(stepTwo);
            builder.alwaysClose(stepTwo);
            return builder.build();
        }
    }

    public static class BranchingWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            DecisionStep decision = new DecisionStep();
            SuccessPath successPath = new SuccessPath();
            FailurePath failurePath = new FailurePath();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(decision);
            builder.commonTransitions(decision, successPath, failurePath);
            builder.addStep(successPath);
            builder.alwaysClose(successPath);
            builder.addStep(failurePath);
            builder.alwaysClose(failurePath);
            return builder.build();
        }
    }

    public static class SingleStepCloseWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            OnlyStep onlyStep = new OnlyStep();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(onlyStep);
            builder.closeOnSuccess(onlyStep);
            return builder.build();
        }
    }

    public static class PartitionedWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            PartStep partStep = new PartStep();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(partStep);
            builder.alwaysClose(partStep);
            return builder.build();
        }
    }

    public static class MultiStepBranchWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            StepA stepA = new StepA();
            StepB stepB = new StepB();
            StepC stepC = new StepC();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepA);
            builder.commonTransitions(stepA, stepB, stepC);
            builder.addStep(stepB);
            builder.alwaysClose(stepB);
            builder.addStep(stepC);
            builder.alwaysClose(stepC);
            return builder.build();
        }
    }

    public static class SucceedOrFailWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            MayFailStep mayFail = new MayFailStep();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(mayFail);
            builder.closeOnSuccess(mayFail);
            builder.closeOnFailure(mayFail);
            return builder.build();
        }
    }
}
