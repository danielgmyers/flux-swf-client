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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.danielgmyers.flux.ex.AttributeTypeMismatchException;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.ex.UnsupportedAttributeTypeException;
import com.danielgmyers.flux.step.StepInputAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class SfnStepInputAccessor implements StepInputAccessor {

    // Map is supported too but we have to check it separately since we don't want to require a specific Map implementation.
    static final Set<Class<?>> SUPPORTED_ATTRIBUTE_TYPES
            = Set.of(Boolean.class, Long.class, String.class, Instant.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ObjectNode attributes;

    // package-private for unit test access
    ObjectNode getAttributesNode() {
        return attributes;
    }

    public SfnStepInputAccessor(String rawInput) throws JsonProcessingException {
        if (rawInput == null || rawInput.isEmpty()) {
            attributes = MAPPER.createObjectNode();
        } else {
            JsonNode input = MAPPER.readTree(rawInput);
            if (!input.isObject()) {
                throw new FluxException("Expected step input to be a json map, but it was not: " + rawInput);
            }
            attributes = (ObjectNode) input;
        }
    }

    /**
     * Retrieves the requested attribute and validates that it's compatible with the stored value.
     * Note that we store Instants as millis-since-epoch, as Longs.
     * As such we can't really tell the difference between Long and Instant, so in practice
     * they'll work interchangeably (whether the resulting value makes sense or not).
     */
    @Override
    public <T> T getAttribute(Class<T> requestedType, String attributeName) {
        if (!SUPPORTED_ATTRIBUTE_TYPES.contains(requestedType) && !Map.class.isAssignableFrom(requestedType)) {
            throw new UnsupportedAttributeTypeException(requestedType, attributeName);
        }

        JsonNode attr = attributes.get(attributeName);
        if (attr == null || attr.isNull()) {
            return null;
        }
        if (requestedType == Boolean.class) {
            return (T)validateAttr(requestedType, attributeName, attr, attr::isBoolean, attr::asBoolean);
        }
        if (requestedType == Long.class) {
            return (T)validateAttr(requestedType, attributeName, attr, attr::canConvertToLong, attr::asLong);
        }
        if (requestedType == String.class) {
            return (T)validateAttr(requestedType, attributeName, attr, attr::isTextual, attr::asText);
        }
        if (requestedType == Instant.class) {
            Long epochMillis = validateAttr(Instant.class, attributeName, attr, attr::canConvertToLong, attr::asLong);
            return (T)Instant.ofEpochMilli(epochMillis);
        }

        // Only support Map<String, String>, not other map types
        if (Map.class.isAssignableFrom(requestedType)) {
            Map<String, String> output = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields
                    = validateAttr(Map.class, attributeName, attr, attr::isObject, attr::fields);
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String subAttributeName = String.format("%s.%s", attributeName, field.getKey());
                JsonNode subAttr = field.getValue();
                String value = null;
                if (!subAttr.isNull()) {
                    value = validateAttr(String.class, subAttributeName, subAttr, subAttr::isTextual, subAttr::asText);
                }
                output.put(field.getKey(), value);
            }
            return (T)output;
        }

        throw new IllegalStateException("Missing implementation for a supported type " + requestedType.getSimpleName() + "!");
    }

    public void addAttribute(String attributeName, Object value) {
        if (!SUPPORTED_ATTRIBUTE_TYPES.contains(value.getClass()) && !Map.class.isAssignableFrom(value.getClass())) {
            throw new UnsupportedAttributeTypeException(value.getClass(), attributeName);
        }

        if (value.getClass() == Boolean.class) {
            attributes.set(attributeName, BooleanNode.valueOf((boolean)value));
        } else if (value.getClass() == Long.class) {
            attributes.set(attributeName, LongNode.valueOf((long)value));
        } else if (value.getClass() == String.class) {
            attributes.set(attributeName, TextNode.valueOf((String)value));
        } else if (value.getClass() == Instant.class) {
            attributes.set(attributeName, LongNode.valueOf(((Instant)value).toEpochMilli()));
        } else if (Map.class.isAssignableFrom(value.getClass())) {
            ObjectNode child = MAPPER.createObjectNode();
            for (Map.Entry<String, String> e : ((Map<String, String>)value).entrySet()) {
                child.set(e.getKey(), TextNode.valueOf(e.getValue()));
            }
            attributes.set(attributeName, child);
        } else {
            throw new IllegalStateException("Missing implementation for a supported type "
                                            + value.getClass().getSimpleName() + "!");
        }
    }

    public void addAttributes(Map<String, Object> data) {
        for (Map.Entry<String, Object> e : data.entrySet()) {
            addAttribute(e.getKey(), e.getValue());
        }
    }

    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(attributes);
    }

    private <T> T validateAttr(Class<?> requestedType, String attributeName, JsonNode node,
                               Supplier<Boolean> validator, Supplier<T> retriever) {
        if (!validator.get()) {
            throw new AttributeTypeMismatchException(requestedType, attributeName, node.getNodeType().toString());
        }
        return retriever.get();
    }
}
