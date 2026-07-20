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

package com.danielgmyers.flux.clients.sfn.asl.state;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AslStateDeserializationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String loadResource(String path) throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/asl/" + path)) {
            Assertions.assertNotNull(is, "Resource not found: /asl/" + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testTaskStateDeserialization() throws IOException {
        String json = loadResource("task-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(TaskState.class, state);
        TaskState task = (TaskState) state;

        Assertions.assertEquals("Task", task.getType());
        Assertions.assertEquals("Task State example", task.getComment());
        Assertions.assertEquals("arn:aws:states:us-east-1:123456789012:activity:HelloWorld", task.getResource());
        Assertions.assertEquals("$.input", task.getInputPath());
        Assertions.assertEquals("$.output", task.getOutputPath());
        Assertions.assertEquals("$.taskResult", task.getResultPath());
        Assertions.assertEquals(300, task.getTimeoutSeconds());
        Assertions.assertEquals(60, task.getHeartbeatSeconds());
        Assertions.assertEquals("NextState", task.getNext());
        Assertions.assertNull(task.getEnd());

        // Parameters
        Assertions.assertNotNull(task.getParameters());
        Assertions.assertEquals("$.data", task.getParameters().get("payload.$"));
        Assertions.assertEquals("hello", task.getParameters().get("staticValue"));

        // ResultSelector
        Assertions.assertNotNull(task.getResultSelector());
        Assertions.assertEquals("$.statusCode", task.getResultSelector().get("status.$"));

        // Credentials
        Assertions.assertNotNull(task.getCredentials());
        Assertions.assertEquals("arn:aws:iam::123456789012:role/MyRole", task.getCredentials().get("RoleArn"));

        // Retry
        Assertions.assertNotNull(task.getRetry());
        Assertions.assertEquals(2, task.getRetry().size());

        Retrier retrier1 = task.getRetry().get(0);
        Assertions.assertEquals(List.of("States.Timeout"), retrier1.getErrorEquals());
        Assertions.assertEquals(3, retrier1.getIntervalSeconds());
        Assertions.assertEquals(2, retrier1.getMaxAttempts());
        Assertions.assertEquals(2.0, retrier1.getBackoffRate());
        Assertions.assertEquals(10, retrier1.getMaxDelaySeconds());
        Assertions.assertEquals("FULL", retrier1.getJitterStrategy());

        Retrier retrier2 = task.getRetry().get(1);
        Assertions.assertEquals(List.of("States.ALL"), retrier2.getErrorEquals());
        Assertions.assertEquals(5, retrier2.getMaxAttempts());
        Assertions.assertNull(retrier2.getIntervalSeconds());
        Assertions.assertNull(retrier2.getBackoffRate());

        // Catch
        Assertions.assertNotNull(task.getCatchers());
        Assertions.assertEquals(2, task.getCatchers().size());

        Catcher catcher1 = task.getCatchers().get(0);
        Assertions.assertEquals(List.of("java.lang.Exception"), catcher1.getErrorEquals());
        Assertions.assertEquals("$.error-info", catcher1.getResultPath());
        Assertions.assertEquals("RecoveryState", catcher1.getNext());

        Catcher catcher2 = task.getCatchers().get(1);
        Assertions.assertEquals(List.of("States.ALL"), catcher2.getErrorEquals());
        Assertions.assertNull(catcher2.getResultPath());
        Assertions.assertEquals("FailState", catcher2.getNext());
    }

    @Test
    public void testPassStateDeserialization() throws IOException {
        String json = loadResource("pass-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(PassState.class, state);
        PassState pass = (PassState) state;

        Assertions.assertEquals("Pass", pass.getType());
        Assertions.assertEquals("Inject test data", pass.getComment());
        Assertions.assertEquals("$.coords", pass.getResultPath());
        Assertions.assertEquals("$.detail", pass.getInputPath());
        Assertions.assertEquals("$.result", pass.getOutputPath());
        Assertions.assertEquals("ProcessData", pass.getNext());

        // Result is a map
        Assertions.assertNotNull(pass.getResult());
        Assertions.assertInstanceOf(Map.class, pass.getResult());
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) pass.getResult();
        Assertions.assertEquals(0.381018, ((Number) result.get("x-datum")).doubleValue(), 0.000001);
        Assertions.assertEquals(622.2269926397355, ((Number) result.get("y-datum")).doubleValue(), 0.0000001);

        // Parameters
        Assertions.assertNotNull(pass.getParameters());
        Assertions.assertEquals(true, pass.getParameters().get("fixed"));
        Assertions.assertEquals("$.origin", pass.getParameters().get("source.$"));
    }

    @Test
    public void testChoiceStateDeserialization() throws IOException {
        String json = loadResource("choice-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(ChoiceState.class, state);
        ChoiceState choice = (ChoiceState) state;

        Assertions.assertEquals("Choice", choice.getType());
        Assertions.assertEquals("Route based on type and value", choice.getComment());
        Assertions.assertEquals("$.request", choice.getInputPath());
        Assertions.assertEquals("$.response", choice.getOutputPath());
        Assertions.assertEquals("DefaultState", choice.getDefaultState());
        Assertions.assertNull(choice.getNext()); // Choice states don't have Next
        Assertions.assertNull(choice.getEnd());  // Choice states can't be End

        Assertions.assertNotNull(choice.getChoices());
        Assertions.assertEquals(7, choice.getChoices().size());

        // Rule 0: Not expression
        ChoiceRule rule0 = choice.getChoices().get(0);
        Assertions.assertEquals("Public", rule0.getNext());
        Assertions.assertNotNull(rule0.getNot());
        Assertions.assertEquals("$.type", rule0.getNot().getVariable());
        Assertions.assertEquals("Private", rule0.getNot().getStringEquals());

        // Rule 1: And expression
        ChoiceRule rule1 = choice.getChoices().get(1);
        Assertions.assertEquals("ValueInTwenties", rule1.getNext());
        Assertions.assertNotNull(rule1.getAnd());
        Assertions.assertEquals(4, rule1.getAnd().size());
        Assertions.assertEquals("$.value", rule1.getAnd().get(0).getVariable());
        Assertions.assertEquals(true, rule1.getAnd().get(0).getIsPresent());
        Assertions.assertEquals(true, rule1.getAnd().get(1).getIsNumeric());
        Assertions.assertEquals(20, rule1.getAnd().get(2).getNumericGreaterThanEquals().intValue());
        Assertions.assertEquals(30, rule1.getAnd().get(3).getNumericLessThan().intValue());

        // Rule 2: NumericGreaterThanPath
        ChoiceRule rule2 = choice.getChoices().get(2);
        Assertions.assertEquals("StartAudit", rule2.getNext());
        Assertions.assertEquals("$.rating", rule2.getVariable());
        Assertions.assertEquals("$.auditThreshold", rule2.getNumericGreaterThanPath());

        // Rule 3: Or expression
        ChoiceRule rule3 = choice.getChoices().get(3);
        Assertions.assertEquals("ProcessActive", rule3.getNext());
        Assertions.assertNotNull(rule3.getOr());
        Assertions.assertEquals(2, rule3.getOr().size());
        Assertions.assertEquals("ACTIVE", rule3.getOr().get(0).getStringEquals());
        Assertions.assertEquals("PENDING", rule3.getOr().get(1).getStringEquals());

        // Rule 4: TimestampGreaterThan
        ChoiceRule rule4 = choice.getChoices().get(4);
        Assertions.assertEquals("RecentEvent", rule4.getNext());
        Assertions.assertEquals("$.timestamp", rule4.getVariable());
        Assertions.assertEquals("2024-01-01T00:00:00Z", rule4.getTimestampGreaterThan());

        // Rule 5: BooleanEquals
        ChoiceRule rule5 = choice.getChoices().get(5);
        Assertions.assertEquals("EnabledPath", rule5.getNext());
        Assertions.assertEquals("$.enabled", rule5.getVariable());
        Assertions.assertEquals(true, rule5.getBooleanEquals());

        // Rule 6: StringMatches
        ChoiceRule rule6 = choice.getChoices().get(6);
        Assertions.assertEquals("ProductionPath", rule6.getNext());
        Assertions.assertEquals("$.name", rule6.getVariable());
        Assertions.assertEquals("prod-*", rule6.getStringMatches());
    }

    @Test
    public void testWaitStateSecondsDeserialization() throws IOException {
        String json = loadResource("wait-state-seconds.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(WaitState.class, state);
        WaitState wait = (WaitState) state;

        Assertions.assertEquals("Wait", wait.getType());
        Assertions.assertEquals("Wait for expiry", wait.getComment());
        Assertions.assertEquals(10, wait.getSeconds());
        Assertions.assertNull(wait.getSecondsPath());
        Assertions.assertNull(wait.getTimestamp());
        Assertions.assertNull(wait.getTimestampPath());
        Assertions.assertEquals("NextState", wait.getNext());
    }

    @Test
    public void testWaitStateTimestampPathDeserialization() throws IOException {
        String json = loadResource("wait-state-timestamp-path.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(WaitState.class, state);
        WaitState wait = (WaitState) state;

        Assertions.assertEquals("Wait", wait.getType());
        Assertions.assertEquals("Wait until timestamp from input", wait.getComment());
        Assertions.assertNull(wait.getSeconds());
        Assertions.assertNull(wait.getSecondsPath());
        Assertions.assertNull(wait.getTimestamp());
        Assertions.assertEquals("$.expirydate", wait.getTimestampPath());
        Assertions.assertEquals("NextState", wait.getNext());
    }

    @Test
    public void testSucceedStateDeserialization() throws IOException {
        String json = loadResource("succeed-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(SucceedState.class, state);
        SucceedState succeed = (SucceedState) state;

        Assertions.assertEquals("Succeed", succeed.getType());
        Assertions.assertEquals("Workflow completed successfully", succeed.getComment());
        Assertions.assertEquals("$.final", succeed.getInputPath());
        Assertions.assertEquals("$.summary", succeed.getOutputPath());
        Assertions.assertNull(succeed.getNext());
        Assertions.assertNull(succeed.getEnd());
    }

    @Test
    public void testFailStateDeserialization() throws IOException {
        String json = loadResource("fail-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(FailState.class, state);
        FailState fail = (FailState) state;

        Assertions.assertEquals("Fail", fail.getType());
        Assertions.assertEquals("Something went wrong", fail.getComment());
        Assertions.assertEquals("CustomError", fail.getError());
        Assertions.assertEquals("The operation failed due to invalid input", fail.getCause());
        Assertions.assertNull(fail.getErrorPath());
        Assertions.assertNull(fail.getCausePath());
        Assertions.assertNull(fail.getNext());
    }

    @Test
    public void testFailStatePathsDeserialization() throws IOException {
        String json = loadResource("fail-state-paths.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(FailState.class, state);
        FailState fail = (FailState) state;

        Assertions.assertEquals("Fail", fail.getType());
        Assertions.assertEquals("Dynamic error info", fail.getComment());
        Assertions.assertNull(fail.getError());
        Assertions.assertNull(fail.getCause());
        Assertions.assertEquals("$.errorCode", fail.getErrorPath());
        Assertions.assertEquals("$.errorMessage", fail.getCausePath());
    }

    @Test
    public void testParallelStateDeserialization() throws IOException {
        String json = loadResource("parallel-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(ParallelState.class, state);
        ParallelState parallel = (ParallelState) state;

        Assertions.assertEquals("Parallel", parallel.getType());
        Assertions.assertEquals("Look up customer info in parallel", parallel.getComment());
        Assertions.assertEquals("$.customer", parallel.getInputPath());
        Assertions.assertEquals("$.lookupResults", parallel.getOutputPath());
        Assertions.assertEquals("$.lookupResults", parallel.getResultPath());
        Assertions.assertEquals("FormatResults", parallel.getNext());

        // Parameters
        Assertions.assertNotNull(parallel.getParameters());
        Assertions.assertEquals("$.id", parallel.getParameters().get("customerId.$"));

        // ResultSelector
        Assertions.assertNotNull(parallel.getResultSelector());
        Assertions.assertEquals("$", parallel.getResultSelector().get("combined.$"));

        // Branches
        Assertions.assertNotNull(parallel.getBranches());
        Assertions.assertEquals(2, parallel.getBranches().size());

        ParallelState.Branch branch1 = parallel.getBranches().get(0);
        Assertions.assertEquals("LookupAddress", branch1.getStartAt());
        Assertions.assertNotNull(branch1.getStates());
        Assertions.assertEquals(1, branch1.getStates().size());
        AslState branchState1 = branch1.getStates().get("LookupAddress");
        Assertions.assertInstanceOf(TaskState.class, branchState1);
        Assertions.assertEquals("arn:aws:lambda:us-east-1:123456789012:function:AddressFinder",
                                ((TaskState) branchState1).getResource());
        Assertions.assertEquals(true, branchState1.getEnd());

        ParallelState.Branch branch2 = parallel.getBranches().get(1);
        Assertions.assertEquals("LookupPhone", branch2.getStartAt());
        AslState branchState2 = branch2.getStates().get("LookupPhone");
        Assertions.assertInstanceOf(TaskState.class, branchState2);
        Assertions.assertEquals("arn:aws:lambda:us-east-1:123456789012:function:PhoneFinder",
                                ((TaskState) branchState2).getResource());

        // Retry
        Assertions.assertNotNull(parallel.getRetry());
        Assertions.assertEquals(1, parallel.getRetry().size());
        Assertions.assertEquals(List.of("States.ALL"), parallel.getRetry().get(0).getErrorEquals());
        Assertions.assertEquals(1, parallel.getRetry().get(0).getIntervalSeconds());
        Assertions.assertEquals(3, parallel.getRetry().get(0).getMaxAttempts());

        // Catch
        Assertions.assertNotNull(parallel.getCatchers());
        Assertions.assertEquals(1, parallel.getCatchers().size());
        Assertions.assertEquals(List.of("States.ALL"), parallel.getCatchers().get(0).getErrorEquals());
        Assertions.assertEquals("HandleError", parallel.getCatchers().get(0).getNext());
    }

    @Test
    public void testMapStateDeserialization() throws IOException {
        String json = loadResource("map-state.json");
        AslState state = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(MapState.class, state);
        MapState map = (MapState) state;

        Assertions.assertEquals("Map", map.getType());
        Assertions.assertEquals("Validate all shipped items", map.getComment());
        Assertions.assertEquals("$.detail", map.getInputPath());
        Assertions.assertEquals("$.detail", map.getOutputPath());
        Assertions.assertEquals("$.shipped", map.getItemsPath());
        Assertions.assertEquals(10, map.getMaxConcurrency());
        Assertions.assertEquals(5, map.getToleratedFailurePercentage().intValue());
        Assertions.assertEquals(3, map.getToleratedFailureCount());
        Assertions.assertEquals("$.detail.shipped", map.getResultPath());
        Assertions.assertEquals("Summarize", map.getNext());

        // ItemSelector
        Assertions.assertNotNull(map.getItemSelector());
        Assertions.assertEquals("$$.Map.Item.Value", map.getItemSelector().get("parcel.$"));
        Assertions.assertEquals("$.delivery-partner", map.getItemSelector().get("courier.$"));

        // ResultSelector
        Assertions.assertNotNull(map.getResultSelector());
        Assertions.assertEquals("$", map.getResultSelector().get("validated.$"));

        // ItemProcessor
        Assertions.assertNotNull(map.getItemProcessor());
        Assertions.assertEquals("Validate", map.getItemProcessor().getStartAt());
        Assertions.assertNotNull(map.getItemProcessor().getProcessorConfig());
        Assertions.assertEquals("DISTRIBUTED", map.getItemProcessor().getProcessorConfig().get("Mode"));
        Assertions.assertEquals("STANDARD", map.getItemProcessor().getProcessorConfig().get("ExecutionType"));

        Assertions.assertNotNull(map.getItemProcessor().getStates());
        Assertions.assertEquals(1, map.getItemProcessor().getStates().size());
        AslState validateState = map.getItemProcessor().getStates().get("Validate");
        Assertions.assertInstanceOf(TaskState.class, validateState);
        Assertions.assertEquals("arn:aws:lambda:us-east-1:123456789012:function:ship-val",
                                ((TaskState) validateState).getResource());
        Assertions.assertEquals(true, validateState.getEnd());

        // Retry
        Assertions.assertNotNull(map.getRetry());
        Assertions.assertEquals(1, map.getRetry().size());
        Assertions.assertEquals(List.of("States.TaskFailed"), map.getRetry().get(0).getErrorEquals());
        Assertions.assertEquals(2, map.getRetry().get(0).getIntervalSeconds());
        Assertions.assertEquals(3, map.getRetry().get(0).getMaxAttempts());
        Assertions.assertEquals(2.0, map.getRetry().get(0).getBackoffRate());

        // Catch
        Assertions.assertNotNull(map.getCatchers());
        Assertions.assertEquals(1, map.getCatchers().size());
        Assertions.assertEquals(List.of("States.ALL"), map.getCatchers().get(0).getErrorEquals());
        Assertions.assertEquals("$.mapError", map.getCatchers().get(0).getResultPath());
        Assertions.assertEquals("HandleMapError", map.getCatchers().get(0).getNext());
    }

    @Test
    public void testRoundTripSerialization() throws IOException {
        // Create a TaskState, serialize it, deserialize it, and verify equality
        TaskState original = new TaskState();
        original.setResource("arn:aws:states:us-east-1:123456789012:activity:MyActivity");
        original.setComment("Round-trip test");
        original.setNext("Done");
        original.setTimeoutSeconds(120);
        original.setHeartbeatSeconds(30);
        original.setInputPath("$.input");
        original.setResultPath("$.result");
        original.setParameters(Map.of("key", "value"));

        Retrier retrier = new Retrier();
        retrier.setErrorEquals(List.of("States.Timeout"));
        retrier.setIntervalSeconds(5);
        retrier.setMaxAttempts(3);
        retrier.setBackoffRate(1.5);
        original.setRetry(List.of(retrier));

        Catcher catcher = new Catcher();
        catcher.setErrorEquals(List.of("States.ALL"));
        catcher.setNext("ErrorHandler");
        catcher.setResultPath("$.errorInfo");
        original.setCatchers(List.of(catcher));

        String json = MAPPER.writeValueAsString(original);
        AslState deserialized = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(TaskState.class, deserialized);
        TaskState roundTripped = (TaskState) deserialized;

        Assertions.assertEquals("Task", roundTripped.getType());
        Assertions.assertEquals(original.getResource(), roundTripped.getResource());
        Assertions.assertEquals(original.getComment(), roundTripped.getComment());
        Assertions.assertEquals(original.getNext(), roundTripped.getNext());
        Assertions.assertEquals(original.getTimeoutSeconds(), roundTripped.getTimeoutSeconds());
        Assertions.assertEquals(original.getHeartbeatSeconds(), roundTripped.getHeartbeatSeconds());
        Assertions.assertEquals(original.getInputPath(), roundTripped.getInputPath());
        Assertions.assertEquals(original.getResultPath(), roundTripped.getResultPath());
        Assertions.assertEquals(original.getParameters(), roundTripped.getParameters());

        Assertions.assertEquals(1, roundTripped.getRetry().size());
        Assertions.assertEquals(List.of("States.Timeout"), roundTripped.getRetry().get(0).getErrorEquals());
        Assertions.assertEquals(5, roundTripped.getRetry().get(0).getIntervalSeconds());
        Assertions.assertEquals(3, roundTripped.getRetry().get(0).getMaxAttempts());
        Assertions.assertEquals(1.5, roundTripped.getRetry().get(0).getBackoffRate());

        Assertions.assertEquals(1, roundTripped.getCatchers().size());
        Assertions.assertEquals(List.of("States.ALL"), roundTripped.getCatchers().get(0).getErrorEquals());
        Assertions.assertEquals("ErrorHandler", roundTripped.getCatchers().get(0).getNext());
        Assertions.assertEquals("$.errorInfo", roundTripped.getCatchers().get(0).getResultPath());
    }

    @Test
    public void testRoundTripChoiceState() throws IOException {
        ChoiceState original = new ChoiceState();
        original.setComment("Simple choice");
        original.setDefaultState("FallbackState");

        ChoiceRule rule = new ChoiceRule();
        rule.setVariable("$.status");
        rule.setStringEquals("ACTIVE");
        rule.setNext("ActiveHandler");
        original.setChoices(List.of(rule));

        String json = MAPPER.writeValueAsString(original);
        AslState deserialized = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(ChoiceState.class, deserialized);
        ChoiceState roundTripped = (ChoiceState) deserialized;

        Assertions.assertEquals("Choice", roundTripped.getType());
        Assertions.assertEquals("Simple choice", roundTripped.getComment());
        Assertions.assertEquals("FallbackState", roundTripped.getDefaultState());
        Assertions.assertEquals(1, roundTripped.getChoices().size());
        Assertions.assertEquals("$.status", roundTripped.getChoices().get(0).getVariable());
        Assertions.assertEquals("ACTIVE", roundTripped.getChoices().get(0).getStringEquals());
        Assertions.assertEquals("ActiveHandler", roundTripped.getChoices().get(0).getNext());
    }

    @Test
    public void testRoundTripParallelState() throws IOException {
        ParallelState original = new ParallelState();
        original.setComment("Parallel test");
        original.setNext("AfterParallel");

        TaskState branchTask = new TaskState();
        branchTask.setResource("arn:aws:lambda:us-east-1:123456789012:function:Worker");
        branchTask.setEnd(true);

        ParallelState.Branch branch = new ParallelState.Branch();
        branch.setStartAt("Work");
        branch.setStates(Map.of("Work", branchTask));
        original.setBranches(List.of(branch));

        String json = MAPPER.writeValueAsString(original);
        AslState deserialized = MAPPER.readValue(json, AslState.class);

        Assertions.assertInstanceOf(ParallelState.class, deserialized);
        ParallelState roundTripped = (ParallelState) deserialized;

        Assertions.assertEquals("Parallel", roundTripped.getType());
        Assertions.assertEquals(1, roundTripped.getBranches().size());
        Assertions.assertEquals("Work", roundTripped.getBranches().get(0).getStartAt());
        AslState innerState = roundTripped.getBranches().get(0).getStates().get("Work");
        Assertions.assertInstanceOf(TaskState.class, innerState);
        Assertions.assertEquals("arn:aws:lambda:us-east-1:123456789012:function:Worker",
                                ((TaskState) innerState).getResource());
    }

    @Test
    public void testNullFieldsNotSerialized() throws IOException {
        SucceedState succeed = new SucceedState();
        succeed.setComment("Done");

        String json = MAPPER.writeValueAsString(succeed);

        // Should not contain null-valued fields
        Assertions.assertFalse(json.contains("\"Next\""));
        Assertions.assertFalse(json.contains("\"End\""));
        Assertions.assertFalse(json.contains("\"InputPath\""));
        Assertions.assertFalse(json.contains("\"OutputPath\""));

        // Should contain the fields that are set
        Assertions.assertTrue(json.contains("\"Type\""));
        Assertions.assertTrue(json.contains("\"Succeed\""));
        Assertions.assertTrue(json.contains("\"Comment\""));
        Assertions.assertTrue(json.contains("\"Done\""));
    }
}
