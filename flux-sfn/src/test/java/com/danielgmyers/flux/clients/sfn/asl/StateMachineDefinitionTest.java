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

import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.PassState;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
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
}
