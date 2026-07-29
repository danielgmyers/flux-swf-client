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

package com.danielgmyers.flux.step.internal;

import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.step.StepInputAccessor;

/**
 * Wrapper around {@link StepInputAccessor} that also contains an extra bag of properties
 * that can be overridden by step hooks. Used to allow hooks to attach extra attributes
 * when intercepting workflow steps.
 *
 * Extra attributes added to this collection will override existing attributes
 * provided by the delegate.
 */
public final class ExecutionAttributes implements StepInputAccessor {
    private final StepInputAccessor delegate;
    private final Map<String, Object> extra;

    public ExecutionAttributes(StepInputAccessor delegate) {
        this.delegate = delegate;
        this.extra = new HashMap<>();
    }

    @Override
    public <T> T getAttribute(Class<T> requestedType, String attributeName) {
        Object value = extra.get(attributeName);
        if (value != null && requestedType.isAssignableFrom(value.getClass())) {
            return (T)value;
        }

        return delegate.getAttribute(requestedType, attributeName);
    }

    public void putExtraAttribute(String key, Object value) {
        extra.put(key, value);
    }

    public void putExtraAttributes(Map<String, Object> values) {
        extra.putAll(values);
    }

    public Map<String, Object> extraAttributes() {
        return extra;
    }
}
