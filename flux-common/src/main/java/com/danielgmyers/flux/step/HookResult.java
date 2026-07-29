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

package com.danielgmyers.flux.step;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.danielgmyers.flux.step.StepResult.ResultAction;

/**
 * The result of executing a step hook.
 *
 * Returning a {@link #proceed()} object from a {@link StepHook.HookType#PRE} or {@link StepHook.HookType#POST}
 * step hook will have no effect on the steps execution but does allow injecting
 * extra step attributes with {@link #withAttribute(String, Object)}.
 *
 * Returning a {@link #retry()} object from a {@link StepHook.HookType#PRE} step hook will prevent the step
 * from being executed and will schedule a retry. If any step hook returns a retry the step will
 * retry, even if a subsequent step hook returns a {@link #complete()}.
 *
 * Returning a {@link #complete()} object from a {@link StepHook.HookType#PRE} step hook will prevent the step
 * from being executed and will immediately schedule the next step to be executed. Any extra attributes
 * attached to the result will be passed to the next step. When returned from a {@link StepHook.HookType#POST}
 * step hook this allows you to override a step that retried with a successful result.
 *
 * If multiple step hooks are registered their execution order is indeterminate. All step hooks will be executed
 * unless one fails by throwing an exception and does not have {@link StepHook#retryOnFailure()} set to {@code true}.
 *
 * If any step hook returns {@link #retry()} then the step will always retry. If a step hook responds with {@link #complete()}
 * and no step hook responds with {@link #retry()} then the step will be successful, even if the step itself returned a retry.
 * Returning {@link #proceed()} will never affect the response of a step, aside from allowing attachment of extra attributes.
 */
public final class HookResult {
    private final ResultAction action;
    private final String resultCode;
    private final String message;
    private final Map<String, Object> attributes;

    /**
     * Continue execution and do not modify the result of this step.
     *
     * All step hooks after this will be executed as normal, any attributes saved on this result
     * will be added to the steps resulting set of attributes.
     *
     * @return A hook result that represents a 'continue' decision.
     */
    public static HookResult proceed() {
        return new HookResult(null, null, null);
    }

    /**
     * Override the result of the step and return a retry instead.
     *
     * @return A hook result that represents a 'retry' decision.
     */
    public static HookResult retry() {
        return retry(null);
    }

    public static HookResult retry(final String message) {
        return retry(null, message);
    }

    public static HookResult retry(final String resultCode, final String message) {
        return new HookResult(ResultAction.RETRY, resultCode, message);
    }

    /**
     * Override the result of the step and return a success instead.
     *
     * @return A hook result that represents a 'success' decision.
     */
    public static HookResult complete() {
        return complete(null);
    }

    public static HookResult complete(final String message) {
        return complete(null, message);
    }

    public static HookResult complete(final String resultCode, final String message) {
        return new HookResult(ResultAction.COMPLETE, resultCode, message);
    }

    /**
     * Attach an extra attribute to this hook that will be injected into the step attributes.
     *
     * If used in a {@link StepHook.HookType#PRE} hook the attributes will be available to the executing
     * step. If used in a {@link StepHook.HookType#POST} hook the attributes will be available to the next
     * step.
     *
     * @param name The name of the attribute
     * @param value The value of the attribute
     */
    public void addAttribute(final String name, final Object value) {
        throwIfActionIsNotSucceed();
        attributes.put(name, value);
    }

    public HookResult withAttribute(final String name, final Object value) {
        addAttribute(name, value);
        return this;
    }

    public HookResult withAllAttributes(final Map<? extends String, ? extends Object> values) {
        throwIfActionIsNotSucceed();
        attributes.putAll(values);
        return this;
    }

    public ResultAction getAction() {
        return action;
    }

    public String getResultCode() {
        return resultCode;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    private HookResult(final ResultAction action, final String resultCode, final String message) {
        if (action == null && (resultCode != null || message != null)) {
            throw new IllegalArgumentException("Result codes must be null when a hook doesnt override the steps result");
        }

        this.action = action;
        this.resultCode = resultCode;
        this.message = message;
        this.attributes = new HashMap<>();
    }

    private void throwIfActionIsNotSucceed() {
        if (action == ResultAction.RETRY) {
            throw new IllegalArgumentException("Attributes cannot be added to Retry or Continue step results.");
        }
    }

    @Override
    public String toString() {
        // Attributes are omitted from the string here, in case they contain sensitive information.
        return "HookResult[action="
            + action
            + ", resultCode=" + resultCode
            + ", message=" + message + "]";
    }
}
