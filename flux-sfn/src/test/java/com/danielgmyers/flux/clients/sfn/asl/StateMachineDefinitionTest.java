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

package com.danielgmyers.flux.clients.sfn.asl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.asl.compiler.WorkflowGraphCompiler;
import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceRule;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceState;
import com.danielgmyers.flux.clients.sfn.asl.state.FailState;
import com.danielgmyers.flux.clients.sfn.asl.state.PassState;
import com.danielgmyers.flux.clients.sfn.asl.state.SucceedState;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StateMachineDefinitionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        String json;
        try (InputStream is = getClass().getResourceAsStream("/asl/state-machine.json")) {
            Assertions.assertNotNull(is);
            json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        StateMachineDefinition def = MAPPER.readValue(json, StateMachineDefinition.class);

        Assertions.assertEquals("A simple Hello World workflow", def.getComment());
        Assertions.assertEquals("HelloWorld", def.getStartAt());
        Assertions.assertEquals("1.0", def.getVersion());
        Assertions.assertEquals(3600, def.getTimeoutSeconds());
        Assertions.assertNull(def.getQueryLanguage());

        Assertions.assertNotNull(def.getStates());
        Assertions.assertEquals(2, def.getStates().size());

        AslState helloState = def.getStates().get("HelloWorld");
        Assertions.assertInstanceOf(TaskState.class, helloState);
        Assertions.assertEquals("arn:aws:lambda:us-east-1:123456789012:function:HelloWorld",
                                ((TaskState) helloState).getResource());
        Assertions.assertEquals("Goodbye", helloState.getNext());

        AslState goodbyeState = def.getStates().get("Goodbye");
        Assertions.assertInstanceOf(PassState.class, goodbyeState);
        Assertions.assertEquals("Done!", ((PassState) goodbyeState).getResult());
        Assertions.assertEquals(true, goodbyeState.getEnd());
    }

    @Test
    public void testRoundTrip() throws IOException {
        StateMachineDefinition original = new StateMachineDefinition();
        original.setComment("Test workflow");
        original.setStartAt("Step1");
        original.setTimeoutSeconds(600);

        TaskState step1 = new TaskState();
        step1.setResource("arn:aws:states:us-east-1:123456789012:activity:DoWork");
        step1.setNext("Step2");

        PassState step2 = new PassState();
        step2.setResult("complete");
        step2.setEnd(true);

        Map<String, AslState> states = new LinkedHashMap<>();
        states.put("Step1", step1);
        states.put("Step2", step2);
        original.setStates(states);

        String json = MAPPER.writeValueAsString(original);
        StateMachineDefinition roundTripped = MAPPER.readValue(json, StateMachineDefinition.class);

        Assertions.assertEquals("Test workflow", roundTripped.getComment());
        Assertions.assertEquals("Step1", roundTripped.getStartAt());
        Assertions.assertEquals(600, roundTripped.getTimeoutSeconds());
        Assertions.assertNull(roundTripped.getVersion());
        Assertions.assertNull(roundTripped.getQueryLanguage());

        Assertions.assertEquals(2, roundTripped.getStates().size());
        Assertions.assertInstanceOf(TaskState.class, roundTripped.getStates().get("Step1"));
        Assertions.assertInstanceOf(PassState.class, roundTripped.getStates().get("Step2"));

        TaskState rt1 = (TaskState) roundTripped.getStates().get("Step1");
        Assertions.assertEquals("arn:aws:states:us-east-1:123456789012:activity:DoWork", rt1.getResource());
        Assertions.assertEquals("Step2", rt1.getNext());

        PassState rt2 = (PassState) roundTripped.getStates().get("Step2");
        Assertions.assertEquals("complete", rt2.getResult());
        Assertions.assertEquals(true, rt2.getEnd());
    }

    @Test
    public void testNullFieldsOmitted() throws IOException {
        StateMachineDefinition def = new StateMachineDefinition();
        def.setStartAt("OnlyState");
        def.setStates(Map.of("OnlyState", new PassState()));

        String json = MAPPER.writeValueAsString(def);

        Assertions.assertTrue(json.contains("\"StartAt\""));
        Assertions.assertTrue(json.contains("\"States\""));
        Assertions.assertFalse(json.contains("\"Comment\""));
        Assertions.assertFalse(json.contains("\"Version\""));
        Assertions.assertFalse(json.contains("\"TimeoutSeconds\""));
        Assertions.assertFalse(json.contains("\"QueryLanguage\""));
    }

    /**
     * Compiles a nontrivial workflow (branching with three steps) through the full pipeline
     * and validates that the serialized JSON is structurally correct ASL.
     */
    @Test
    public void testCompiledWorkflowProducesValidAsl() throws IOException {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion("us-east-1");
        config.setAwsAccountId("111222333444");

        WorkflowGraphCompiler compiler = new WorkflowGraphCompiler(config);
        StateMachineDefinition def = compiler.compile(new OrderProcessingWorkflow());

        String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(def);
        JsonNode root = MAPPER.readTree(json);

        // Top-level structure
        Assertions.assertEquals("OrderProcessingWorkflow", root.get("StartAt").asText().split("\\.")[0]);
        Assertions.assertTrue(root.has("States"));
        Assertions.assertTrue(root.has("Comment"));

        JsonNode states = root.get("States");

        // Validate step: Task state with correct resource ARN pattern
        JsonNode validateTask = states.get("OrderProcessingWorkflow.ValidateOrder");
        Assertions.assertNotNull(validateTask, "ValidateOrder Task state should exist");
        Assertions.assertEquals("Task", validateTask.get("Type").asText());
        Assertions.assertTrue(validateTask.get("Resource").asText().contains("activity:"));
        Assertions.assertTrue(validateTask.get("Resource").asText().contains("OrderProcessingWorkflow"));
        Assertions.assertTrue(validateTask.get("Resource").asText().contains("ValidateOrder"));
        Assertions.assertEquals("OrderProcessingWorkflow.ValidateOrder.Route",
                                validateTask.get("Next").asText());

        // Verify Retry is present on the Task
        Assertions.assertTrue(validateTask.has("Retry"));
        JsonNode retryArray = validateTask.get("Retry");
        Assertions.assertTrue(retryArray.isArray());
        Assertions.assertTrue(retryArray.size() >= 1);
        Assertions.assertEquals("States.TaskFailed",
                                retryArray.get(0).get("ErrorEquals").get(0).asText());

        // Choice state routes correctly
        JsonNode choiceState = states.get("OrderProcessingWorkflow.ValidateOrder.Route");
        Assertions.assertNotNull(choiceState, "Route Choice state should exist");
        Assertions.assertEquals("Choice", choiceState.get("Type").asText());
        Assertions.assertTrue(choiceState.has("Choices"));
        Assertions.assertTrue(choiceState.has("Default"));
        Assertions.assertEquals("OrderProcessingWorkflow.ValidateOrder.BadResult",
                                choiceState.get("Default").asText());

        // Choice rules reference the result code path and target correct states
        JsonNode choices = choiceState.get("Choices");
        boolean foundSucceed = false;
        boolean foundFail = false;
        for (JsonNode rule : choices) {
            Assertions.assertEquals("$._flux_resultCode", rule.get("Variable").asText());
            String target = rule.get("Next").asText();
            if ("_succeed".equals(rule.get("StringEquals").asText())) {
                Assertions.assertEquals("OrderProcessingWorkflow.FulfillOrder", target);
                foundSucceed = true;
            } else if ("_fail".equals(rule.get("StringEquals").asText())) {
                Assertions.assertEquals("OrderProcessingWorkflow.RejectOrder", target);
                foundFail = true;
            }
        }
        Assertions.assertTrue(foundSucceed, "Should have a _succeed choice rule");
        Assertions.assertTrue(foundFail, "Should have a _fail choice rule");

        // Fail state for unrecognized result codes
        JsonNode badResult = states.get("OrderProcessingWorkflow.ValidateOrder.BadResult");
        Assertions.assertNotNull(badResult);
        Assertions.assertEquals("Fail", badResult.get("Type").asText());
        Assertions.assertEquals("Flux.UnrecognizedResultCode", badResult.get("Error").asText());

        // FulfillOrder and RejectOrder both transition to Succeed
        JsonNode fulfillTask = states.get("OrderProcessingWorkflow.FulfillOrder");
        Assertions.assertNotNull(fulfillTask);
        Assertions.assertEquals("Task", fulfillTask.get("Type").asText());
        Assertions.assertEquals("OrderProcessingWorkflow.Succeed", fulfillTask.get("Next").asText());

        JsonNode rejectTask = states.get("OrderProcessingWorkflow.RejectOrder");
        Assertions.assertNotNull(rejectTask);
        Assertions.assertEquals("Task", rejectTask.get("Type").asText());
        Assertions.assertEquals("OrderProcessingWorkflow.Succeed", rejectTask.get("Next").asText());

        // Terminal Succeed state
        JsonNode succeed = states.get("OrderProcessingWorkflow.Succeed");
        Assertions.assertNotNull(succeed);
        Assertions.assertEquals("Succeed", succeed.get("Type").asText());

        // Verify the JSON can be deserialized back into a valid StateMachineDefinition
        StateMachineDefinition roundTripped = MAPPER.readValue(json, StateMachineDefinition.class);
        Assertions.assertEquals(def.getStartAt(), roundTripped.getStartAt());
        Assertions.assertEquals(def.getStates().size(), roundTripped.getStates().size());
        Assertions.assertInstanceOf(TaskState.class,
                roundTripped.getStates().get("OrderProcessingWorkflow.ValidateOrder"));
        Assertions.assertInstanceOf(ChoiceState.class,
                roundTripped.getStates().get("OrderProcessingWorkflow.ValidateOrder.Route"));
        Assertions.assertInstanceOf(FailState.class,
                roundTripped.getStates().get("OrderProcessingWorkflow.ValidateOrder.BadResult"));
        Assertions.assertInstanceOf(SucceedState.class,
                roundTripped.getStates().get("OrderProcessingWorkflow.Succeed"));
    }

    // --- Test workflow for the compiled ASL test ---

    public static class ValidateOrder implements WorkflowStep {
        @StepApply
        public StepResult validate() {
            return StepResult.success();
        }
    }

    public static class FulfillOrder implements WorkflowStep {
        @StepApply
        public void fulfill() {
        }
    }

    public static class RejectOrder implements WorkflowStep {
        @StepApply
        public void reject() {
        }
    }

    public static class OrderProcessingWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            ValidateOrder validate = new ValidateOrder();
            FulfillOrder fulfill = new FulfillOrder();
            RejectOrder reject = new RejectOrder();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(validate);
            builder.commonTransitions(validate, fulfill, reject);
            builder.addStep(fulfill);
            builder.alwaysClose(fulfill);
            builder.addStep(reject);
            builder.alwaysClose(reject);
            return builder.build();
        }
    }
}
