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

package com.danielgmyers.flux.clients.sfn.step;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.danielgmyers.flux.ex.AttributeTypeMismatchException;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.ex.UnsupportedAttributeTypeException;
import com.danielgmyers.flux.step.StepInputAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SfnStepInputAccessorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testInputIsNotJsonObjectMap() throws JsonProcessingException {
        Assertions.assertThrows(FluxException.class, () -> new SfnStepInputAccessor("1"));
        Assertions.assertThrows(FluxException.class, () -> new SfnStepInputAccessor("[1,2,3,4]"));
        Assertions.assertThrows(FluxException.class, () -> new SfnStepInputAccessor("true"));
    }

    @Test
    public void testNullOrEmptyInputOrEmptyMap() throws JsonProcessingException {
        SfnStepInputAccessor accessor = new SfnStepInputAccessor(null);
        Assertions.assertTrue(accessor.getAttributesNode().isEmpty());

        accessor = new SfnStepInputAccessor("");
        Assertions.assertTrue(accessor.getAttributesNode().isEmpty());

        accessor = new SfnStepInputAccessor("{}");
        Assertions.assertTrue(accessor.getAttributesNode().isEmpty());
    }

    @Test
    public void testNullAndMissingValues() throws JsonProcessingException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", null);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        for (Class<?> supportedType : SfnStepInputAccessor.SUPPORTED_ATTRIBUTE_TYPES) {
            Assertions.assertNull(accessor.getAttribute(supportedType, "entry"));
            Assertions.assertNull(accessor.getAttribute(supportedType, "missing_key"));
        }
    }

    @Test
    public void testLongAndInstantValue() throws JsonProcessingException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("long", 42L);
        attributes.put("timestamp", Instant.now().toEpochMilli());

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        for (String key : attributes.keySet()) {
            long value = (long)(attributes.get(key));
            for (Class<?> supportedType : SfnStepInputAccessor.SUPPORTED_ATTRIBUTE_TYPES) {
                if (supportedType == Long.class) {
                    Assertions.assertEquals(value, accessor.getAttribute(supportedType, key));
                } else if (supportedType == Instant.class) {
                    Assertions.assertEquals(Instant.ofEpochMilli(value), accessor.getAttribute(supportedType, key));
                } else {
                    Assertions.assertThrows(AttributeTypeMismatchException.class,
                            () -> accessor.getAttribute(supportedType, key));
                }
            }
        }
    }

    @Test
    public void testStringValue() throws JsonProcessingException {
        String value = "asdfghjkl";
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", value);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        for (Class<?> supportedType : SfnStepInputAccessor.SUPPORTED_ATTRIBUTE_TYPES) {
            if (supportedType == String.class) {
                Assertions.assertEquals(value, accessor.getAttribute(supportedType, "entry"));
            } else {
                Assertions.assertThrows(AttributeTypeMismatchException.class,
                        () -> accessor.getAttribute(supportedType, "entry"));
            }
        }
    }

    @Test
    public void testBooleanValues() throws JsonProcessingException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry1", true);
        attributes.put("entry2", false);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        for (String key : attributes.keySet()) {
            boolean value = (boolean)(attributes.get(key));
            for (Class<?> supportedType : SfnStepInputAccessor.SUPPORTED_ATTRIBUTE_TYPES) {
                if (supportedType == Boolean.class) {
                    Assertions.assertEquals(value, accessor.getAttribute(supportedType, key));
                } else {
                    Assertions.assertThrows(AttributeTypeMismatchException.class,
                            () -> accessor.getAttribute(supportedType, key));
                }
            }
        }
    }

    @Test
    public void testStringMapValue() throws JsonProcessingException {
        Map<String, String> subMap = new HashMap<>();
        subMap.put("a", "b");
        subMap.put("c", null);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", subMap);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        for (Class<?> supportedType : SfnStepInputAccessor.SUPPORTED_ATTRIBUTE_TYPES) {
            if (supportedType == Map.class) {
                Assertions.assertEquals(subMap, accessor.getAttribute(supportedType, "entry"));
            } else {
                Assertions.assertThrows(AttributeTypeMismatchException.class,
                        () -> accessor.getAttribute(supportedType, "entry"));
            }
        }
    }

    @Test
    public void testStringMapValue_subMapCannotContainLong() throws JsonProcessingException {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("a", 42L);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", subMap);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        Assertions.assertThrows(AttributeTypeMismatchException.class,
                () -> accessor.getAttribute(Map.class, "entry"));
    }

    @Test
    public void testStringMapValue_subMapCannotContainBoolean() throws JsonProcessingException {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("a", true);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", subMap);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        Assertions.assertThrows(AttributeTypeMismatchException.class,
                () -> accessor.getAttribute(Map.class, "entry"));
    }

    @Test
    public void testStringMapValue_subMapCannotContainMap() throws JsonProcessingException {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("a", new HashMap<>());

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("entry", subMap);

        String encoded = MAPPER.writeValueAsString(attributes);

        StepInputAccessor accessor = new SfnStepInputAccessor(encoded);
        Assertions.assertThrows(AttributeTypeMismatchException.class,
                () -> accessor.getAttribute(Map.class, "entry"));
    }

    @Test
    public void testAddAttribute() throws JsonProcessingException {
        SfnStepInputAccessor accessor = new SfnStepInputAccessor("{}");

        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        Map<String, String> stringmap = new HashMap<>();
        stringmap.put("a", "b");
        stringmap.put("c", "d");

        Map<String, Object> additionalAttributes = new HashMap<>();
        additionalAttributes.put("entry1", "value1");
        additionalAttributes.put("entry2", "value2");

        accessor.addAttribute("bool", true);
        accessor.addAttribute("long", 42L);
        accessor.addAttribute("string", "some value");
        accessor.addAttribute("timestamp", now.toEpochMilli());
        accessor.addAttribute("map", stringmap);
        accessor.addAttributes(additionalAttributes);

        Assertions.assertEquals(true, accessor.getAttribute(Boolean.class, "bool"));
        Assertions.assertEquals(42L, accessor.getAttribute(Long.class, "long"));
        Assertions.assertEquals("some value", accessor.getAttribute(String.class, "string"));
        Assertions.assertEquals(now, accessor.getAttribute(Instant.class, "timestamp"));
        Assertions.assertEquals(stringmap, accessor.getAttribute(Map.class, "map"));
        Assertions.assertEquals("value1", accessor.getAttribute(String.class, "entry1"));
        Assertions.assertEquals("value2", accessor.getAttribute(String.class, "entry2"));
    }

    @Test
    public void testAddUnsupportedAttributeType() throws JsonProcessingException {
        SfnStepInputAccessor accessor = new SfnStepInputAccessor("{}");

        Assertions.assertThrows(UnsupportedAttributeTypeException.class,
                                () -> accessor.addAttribute("bad", new Date()));
        Assertions.assertThrows(UnsupportedAttributeTypeException.class,
                                () -> accessor.addAttribute("bad", new ArrayList<String>()));
        Assertions.assertThrows(UnsupportedAttributeTypeException.class,
                                () -> accessor.addAttribute("bad", new HashSet<String>()));
    }

    @Test
    public void toJson() throws JsonProcessingException {
        SfnStepInputAccessor accessor = new SfnStepInputAccessor("{\"a\":\"b\"}");
        accessor.addAttribute("c", "d");

        Assertions.assertEquals("{\"a\":\"b\",\"c\":\"d\"}", accessor.toJson());
    }
}
