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

package software.amazon.aws.clients.swf.flux.step;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for handling workflow step attribute operations.
 */
public final class StepAttributes {

    public static final String ACTIVITY_COMPLETION_MESSAGE = "_activity_completion_message";
    public static final String ACTIVITY_INITIAL_ATTEMPT_TIME = "_activity_initial_attempt_time";
    public static final String PARTITION_COUNT = "_partition_count";
    public static final String PARTITION_ID = "_partition_id";
    public static final String RESULT_CODE = "_result_code";
    public static final String RETRY_ATTEMPT = "_retry_attempt";
    public static final String WORKFLOW_ID = "_h_workflow_id"; // prefixed with _h because initially it was only given to hooks
    public static final String WORKFLOW_EXECUTION_ID = "_execution_id";
    public static final String WORKFLOW_START_TIME = "_workflow_start_time";

    // These attributes are available only to Step Hook methods.
    public static final String ACTIVITY_NAME = "_h_activity_name";

    /**
     * Returns the expected attribute type when a step or hook requests the specified special attribute.
     * If the specified attribute is not recognized, throws an exception.
     */
    public static Class<?> getSpecialAttributeType(String attributeName) {
        switch (attributeName) {
            case StepAttributes.ACTIVITY_COMPLETION_MESSAGE:
                return String.class;
            case StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME:
                return Instant.class;
            case StepAttributes.RESULT_CODE:
                return String.class;
            case StepAttributes.RETRY_ATTEMPT:
                return Long.class;
            case StepAttributes.PARTITION_COUNT:
                return Long.class;
            case StepAttributes.PARTITION_ID:
                return String.class;
            case StepAttributes.WORKFLOW_ID:
                return String.class;
            case StepAttributes.WORKFLOW_EXECUTION_ID:
                return String.class;
            case StepAttributes.WORKFLOW_START_TIME:
                return Instant.class;
            // Hook-specific attributes below
            case StepAttributes.ACTIVITY_NAME:
                return String.class;
            default:
                throw new IllegalArgumentException("Unrecognized special attribute: " + attributeName);
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Map is allowed but it's not included here because we want to allow any implementation of Map, not just the interface.
    // Visible for testing.
    static final Set<Class<?>> ALLOWED_TYPES
            = Stream.of(Boolean.class, String.class, Long.class, Date.class, Instant.class)
                    .collect(Collectors.toSet());

    private StepAttributes() {}

    /**
     * Given an encoded (json) string, returns a deserialized version of the value.
     * The type may be a Map, but if it is, this method will return a Map&lt;String, String&gt;.
     * Returns null if the encoded string is null or empty.
     * @param type The type to deserialize as
     * @param encoded A json string containing a map.
     * @return An object of the specified type
     */
    public static <T> T decode(Class<T> type, String encoded) {
        try {
            validateAttributeClass(type);

            if (encoded == null || encoded.equals("")) {
                // for convenience, if the requested type was a map, return a matching empty collection.
                if (Map.class == type) {
                    return (T)(new HashMap<>());
                }
                return null;
            } else {
                if (type == Instant.class) {
                    Long value = MAPPER.readValue(encoded, Long.class);
                    return (T)(Instant.ofEpochMilli(value));
                } else if (type == Date.class) {
                    Long value = MAPPER.readValue(encoded, Long.class);
                    return (T)(new Date(value));
                }
                return MAPPER.readValue(encoded, type);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to decode data, expected type " + type.getSimpleName() + ": " + encoded, e);
        }
    }

    /**
     * Generates a json string containing the given object.
     * @param decoded The value to encode
     * @return The json-encoded string
     */
    public static String encode(Object decoded) {
        try {
            if (decoded == null) {
                return null;
            }
            validateAttributeClass(decoded.getClass());
            Object objectToWrite = decoded;
            if (decoded.getClass() == Instant.class) {
                // translate instant to millis, so we can decode it as either instant or time.
                // we lose micro/nano precision, but that's probably ok.
                objectToWrite = ((Instant)decoded).toEpochMilli();
            } else if (decoded.getClass() == Date.class) {
                // translate date to millis, so we can decode it as either instant or time.
                objectToWrite = ((Date)decoded).getTime();
            }
            return MAPPER.writeValueAsString(objectToWrite);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to encode data.", e);
        }
    }

    /**
     * Given a Map&lt;String, ?&gt;, produces a Map&lt;String, String&gt; with the same keys
     * and a json-serialized version of each value.
     * @param rawInput The map whose values need serializing
     * @return A new Map with the serialized values
     */
    public static Map<String, String> serializeMapValues(Map<String, ?> rawInput) {
        Map<String, String> stringMap = new HashMap<>();
        for (Entry<String, ?> entry : rawInput.entrySet()) {
            stringMap.put(entry.getKey(), StepAttributes.encode(entry.getValue()));
        }
        return stringMap;
    }


    /**
     * Throws an IllegalArgumentException if the specified class is not a supported input parameter type for a @StepApply method.
     */
    public static void validateAttributeClass(Class<?> clazz) {
        if (!isValidAttributeClass(clazz)) {
            String message = String.format("Step attributes can be Boolean, Long, String, Date, or Map<String, String>. Was: %s",
                                           clazz.getSimpleName());
            throw new IllegalArgumentException(message);
        }
    }

    // Visible for testing
    static boolean isValidAttributeClass(Class<?> clazz) {
        return ALLOWED_TYPES.contains(clazz) || Map.class.isAssignableFrom(clazz);
    }
}
