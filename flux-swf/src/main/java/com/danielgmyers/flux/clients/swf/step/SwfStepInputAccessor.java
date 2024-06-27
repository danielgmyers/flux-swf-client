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

import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepInputAccessor;

/**
 * Provides access to step attributes which are stored in encoded form.
 */
public class SwfStepInputAccessor implements StepInputAccessor {

    private Map<String, String> encodedAttributes;

    public SwfStepInputAccessor(Map<String, String> encodedAttributes) {
        this.encodedAttributes = encodedAttributes;
    }

    public SwfStepInputAccessor(String serializedInput) {
        if (serializedInput == null || serializedInput.isBlank()) {
            encodedAttributes = Collections.emptyMap();
        } else {
            encodedAttributes = StepAttributes.decode(Map.class, serializedInput);
        }
    }

    public Map<String, String> getEncodedAttributes() {
        return encodedAttributes;
    }

    @Override
    public <T> T getAttribute(Class<T> requestedType, String attributeName) {
        return StepAttributes.decode(requestedType, encodedAttributes.get(attributeName));
    }
}
