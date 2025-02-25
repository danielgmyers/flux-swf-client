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

package com.danielgmyers.flux.clients.swf.step;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.danielgmyers.flux.step.StepAttributes;

public class SwfStepAttributeManagerTest {

    @Test
    public void testEmptyInitialInput() {
        SwfStepAttributeManager ssam = SwfStepAttributeManager.generateInitialStepInput();

        Map<String, Object> expectedInputs = new TreeMap<>();
        expectedInputs.put(SwfStepAttributeManager.STEP_INPUT_METADATA_VERSION_FIELD_NAME, SwfStepAttributeManager.CURRENT_STEP_INPUT_METADATA_VERSION);

        validateEncoding(ssam, expectedInputs);
    }

    @Test
    public void testVariousInputAttributes() {
        Map<String, Object> inputs = new HashMap<>();
        inputs.put("someString", "asdf");
        inputs.put("someLong", 42L);
        inputs.put("someInstant", Instant.now());
        inputs.put("someBoolean", true);

        SwfStepAttributeManager ssam = SwfStepAttributeManager.generateInitialStepInput();
        ssam.addAttributes(inputs);

        Map<String, Object> expectedInputs = new TreeMap<>(inputs);
        expectedInputs.put(SwfStepAttributeManager.STEP_INPUT_METADATA_VERSION_FIELD_NAME, SwfStepAttributeManager.CURRENT_STEP_INPUT_METADATA_VERSION);

        validateEncoding(ssam, expectedInputs);
    }

    @Test
    public void testConstructedFromEncodedString() {
        Instant now = Instant.now();
        String encoded = "{\"_fluxMetadataVersion\":\"1\",\"someBoolean\":\"true\",\"someInstant\":\""
                + now.toEpochMilli() + "\",\"someLong\":\"42\",\"someString\":\"\\\"asdf\\\"\"}";

        SwfStepAttributeManager ssam = new SwfStepAttributeManager(encoded);

        Map<String, Object> expectedInputs = new TreeMap<>();
        expectedInputs.put("someString", "asdf");
        expectedInputs.put("someLong", 42L);
        expectedInputs.put("someInstant", now);
        expectedInputs.put("someBoolean", true);
        expectedInputs.put(SwfStepAttributeManager.STEP_INPUT_METADATA_VERSION_FIELD_NAME, SwfStepAttributeManager.CURRENT_STEP_INPUT_METADATA_VERSION);

        validateEncoding(ssam, expectedInputs);
    }

    private void validateEncoding(SwfStepAttributeManager ssam, Map<String, Object> decodedAttributes) {
        for (Map.Entry<String, Object> attribute : decodedAttributes.entrySet()) {
            Assertions.assertEquals(StepAttributes.encode(attribute.getValue()), ssam.getEncodedAttributes().get(attribute.getKey()));
        }

        Assertions.assertEquals(StepAttributes.encode(ssam.getEncodedAttributes()), ssam.encode());
    }
}
