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

import java.util.Objects;

/**
 * Allows a step or workflow hook to modify the execution of a step. Can be returned by
 * a {@link StepHook} annotated method.
 *
 * Step and workflow hooks are executed in the order they are added. Hook actions
 * are evaluated with the first non-proceed result being the final result. All hooks
 * are evaluated for each step execution and results after a non-proceed result are
 * discarded.
 */
public final class HookResult {
    private enum HookAction {
        CONTINUE,
        RETRY,
        FORCE_RESULT
    }

    private final HookAction action;
    private final String resultCode;
    private final String message;

    /**
     * Continue execution of the step as normal.
     * @return a HookResult instructing Flux to continue normal execution
     */
    public static HookResult proceed() {
        return new HookResult(HookAction.CONTINUE, null, null);
    }

    /**
     * Force the step to be retried.
     *
     * If this is returned from a PRE hook all remaining PRE step hooks are executed, the
     * step body is skipped, and then all POST step hooks are executed. The step is
     * then scheduled for a retry.
     *
     * If this is returned from a POST hook all remaining POST step hooks are executed,
     * and the step is then scheduled for a retry.
     * @return a HookResult instructing Flux to retry the step
     */
    public static HookResult retryStep() {
        return retryStep(null);
    }

    public static HookResult retryStep(String message) {
        return new HookResult(HookAction.RETRY, null, message);
    }

    /**
     * Ignore the steps result and override it with a new result.
     *
     * If this is returned from a PRE hook all remaining PRE step hooks are executed,
     * the step body is skipped, and then all POST step hooks are executed. The steps
     * result is then recorded as this result.
     *
     * If this is returned from a POST hook all remaining POST step hooks are executed,
     * and the steps result is then replaced with this value.
     *
     * @param resultCode the result code to record for the step
     * @return a HookResult instructing Flux to override the step's result
     */
    public static HookResult overrideStepResult(String resultCode) {
        return overrideStepResult(resultCode, null);
    }

    public static HookResult overrideStepResult(String resultCode, String message) {
        return new HookResult(HookAction.FORCE_RESULT, Objects.requireNonNull(resultCode), message);
    }

    private HookResult(HookAction action, String resultCode, String message) {
        this.action = Objects.requireNonNull(action);
        this.resultCode = resultCode;
        this.message = message;
    }

    public boolean isContinue() {
        return action.equals(HookAction.CONTINUE);
    }

    public boolean isRetry() {
        return action.equals(HookAction.RETRY);
    }

    public boolean isForceResult() {
        return action.equals(HookAction.FORCE_RESULT);
    }

    public String getResultCode() {
        return resultCode;
    }

    public String getMessage() {
        return message;
    }
}
