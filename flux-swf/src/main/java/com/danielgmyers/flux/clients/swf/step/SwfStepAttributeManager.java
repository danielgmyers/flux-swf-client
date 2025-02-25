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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepInputAccessor;

/**
 * Provides access to a step's attributes.
 */
public class SwfStepAttributeManager implements StepInputAccessor {

    /**
     * Metadata version history:
     * 1 - Raw map of input attributes encoded as strings, with an optional version number entry.
     *     All attributes, including the version number, are encoded as strings (e.g. 5 -> "5"),
     *     but strings are weird since their encoding includes surrounding quotes (i.e. "abc" -> "\"abc\"").
     */
    static final String STEP_INPUT_METADATA_VERSION_FIELD_NAME = "_fluxMetadataVersion";
    static final Long CURRENT_STEP_INPUT_METADATA_VERSION = 1L;

    private final Map<String, String> encodedAttributes;

    /**
     * Generates initial step input content for a step. Initially only contains the metadata version.
     */
    public static SwfStepAttributeManager generateInitialStepInput() {
        SwfStepAttributeManager input = new SwfStepAttributeManager(Collections.emptyMap());
        input.addAttribute(STEP_INPUT_METADATA_VERSION_FIELD_NAME, CURRENT_STEP_INPUT_METADATA_VERSION);
        return input;
    }

    public SwfStepAttributeManager(String serializedInput) {
        this(StepAttributes.decode(Map.class, serializedInput));
    }

    private SwfStepAttributeManager(Map<String, String> encodedAttributes) {
        this.encodedAttributes = new TreeMap<>(encodedAttributes);
    }

    public Map<String, String> getEncodedAttributes() {
        return encodedAttributes;
    }

    @Override
    public <T> T getAttribute(Class<T> requestedType, String attributeName) {
        return StepAttributes.decode(requestedType, encodedAttributes.get(attributeName));
    }

    public <T> void addAttribute(String attributeName, T decodedValue) {
        encodedAttributes.put(attributeName, StepAttributes.encode(decodedValue));
    }

    public <T> void addAttributes(Map<String, Object> attributes) {
        attributes.forEach(this::addAttribute);
    }

    public void addEncodedAttributes(Map<String, String> encodedAttributes) {
        if (encodedAttributes != null) {
            this.encodedAttributes.putAll(encodedAttributes);
        }
    }

    public void removeAttribute(String attributeName) {
        encodedAttributes.remove(attributeName);
    }

    public String encode() {
        return StepAttributes.encode(encodedAttributes);
    }
}
