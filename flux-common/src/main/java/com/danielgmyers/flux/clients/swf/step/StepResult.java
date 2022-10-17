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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A container object for passing around the results of a workflow step,
 * including whether it succeeded and any attributes that should be passed along to the next step.
 *
 * Output attributes may be of any type allowed by Flux as input attributes;
 * see com.danielgmyers.flux.clients.swf.step.Attribute for more information.
 *
 * Output attributes provided by partitioned steps are ignored.
 *
 * Output attributes overwrite existing attributes if there is a name conflict.
 * This should be done sparingly if at all, since it complicates debugging.
 *
 * The SWF API field used by Flux to store attributes has a maximum size enforced by SWF. Step attributes should
 * be used only to store small pieces of metadata that are not needed beyond the lifetime of the workflow.
 * If the attributes output by a step make the attribute map too large, the step will retry indefinitely
 * until the code is updated to return smaller or fewer attributes in the step result.
 *
 * For convenience, Flux provides two additional attributes in the metadata for the SWF ActivityTaskCompleted event:
 * - "_result_code": String
 *   The result code returned by the step.
 * - "_activity_completion_message": String
 *   The message included in the StepResult, i.e. the string in StepResult.success("Completion message here");
 *
 * The two-parameter overloads of StepResult.success() and StepResult.failure() allow the definition of custom
 * result codes for the step. These result codes can be used with WorkflowGraphBuilder to define branching paths
 * through a workflow's steps. For example, a series of rollback steps may be define which can unwind
 * a partially-applied operation.
 */
public final class StepResult {

    public static final String SUCCEED_RESULT_CODE = "_succeed";
    public static final String FAIL_RESULT_CODE = "_fail";
    public static final String ALWAYS_RESULT_CODE = "_always";

    public static final Set<String> VALID_PARTITIONED_STEP_RESULT_CODES
            = Set.of(SUCCEED_RESULT_CODE, FAIL_RESULT_CODE, ALWAYS_RESULT_CODE);

    /**
     * An enum indicating whether the step succeeded or the workflow should be rolled back.
     */
    public enum ResultAction { COMPLETE, RETRY }

    private final ResultAction action;
    private final String resultCode;
    private final String message;
    private final Map<String, Object> outputAttributes;
    private final Throwable cause;

    /**
     * Constructs a step result directly using the provided action and message.
     *
     * @param action The action corresponding to the step result.
     * @param resultCode The result code associated with this result, used to decide which step to execute next.
     * @param message A message which may provide more information regarding the step result.
     */
    public StepResult(ResultAction action, String resultCode, String message) {
        this.action = action;
        if (action == ResultAction.RETRY && resultCode != null) {
            throw new IllegalArgumentException("Result codes must be null for retries.");
        }
        this.resultCode = resultCode;
        this.message = message;
        this.cause = null;
        outputAttributes = new HashMap<>();
    }

    /**
     * Constructs a retry step result using an exception.
     *
     * @param cause The exception that caused the retry.
     */
    public StepResult(Throwable cause) {
        if (cause == null) {
            throw new IllegalArgumentException("Cause may not be null");
        }
        this.action = ResultAction.RETRY;
        this.resultCode = null;
        this.cause = cause;
        this.message = cause.getMessage();
        this.outputAttributes = Collections.emptyMap();
    }

    /**
     * Produces a result object with a specific result code and message.
     */
    public static StepResult complete(String resultCode, String message) {
        if (resultCode == null || resultCode.isEmpty()) {
            throw new IllegalArgumentException("Result codes cannot be blank.");
        }
        return new StepResult(ResultAction.COMPLETE, resultCode, message);
    }

    /**
     * Produces a success result object with no message.
     */
    public static StepResult success() {
        return success(null);
    }

    /**
     * Produces a success result object with a specific message.
     */
    public static StepResult success(String message) {
        return complete(SUCCEED_RESULT_CODE, message);
    }

    /**
     * Produces a retry result object with no message.
     */
    public static StepResult retry() {
        return new StepResult(ResultAction.RETRY, null, null);
    }

    /**
     * Produces a retry result object with a specific message.
     */
    public static StepResult retry(String message) {
        return new StepResult(ResultAction.RETRY, null, message);
    }

    /**
     * Produces a retry result object caused by a particular exception.
     */
    public static StepResult retry(Throwable cause) {
        return new StepResult(cause);
    }

    /**
     * Produces a failure result object with no message.
     */
    public static StepResult failure() {
        return failure(null);
    }

    /**
     * Produces a failure result object with a specific message.
     */
    public static StepResult failure(String message) {
        return complete(FAIL_RESULT_CODE, message);
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

    public Throwable getCause() {
        return cause;
    }

    /**
     * Allows an additional attribute to be added to the workflow's attribute map.
     * Only allowed if the step result is not a retry.
     */
    public void addAttribute(String attributeName, Object attributeValue) {
        throwIfActionIsRetry();
        outputAttributes.put(attributeName, attributeValue);
    }

    /**
     * Allows an additional attribute to be added to the workflow's attribute map.
     * Only allowed if the step result is not a retry.
     */
    public StepResult withAttribute(String attributeName, Object attributeValue) {
        throwIfActionIsRetry();
        outputAttributes.put(attributeName, attributeValue);
        return this;
    }

    /**
     * Allows additional attributes to be added to the workflow's attribute map.
     * Only allowed if the step result is not a retry.
     */
    public StepResult withAttributes(Map<String, ?> attributes) {
        // this is to facilitate patterns like creating the attribute map variable, populating it with values
        // in the success cases, and then passing that map to the StepResult object regardless of result code,
        // which can result in cleaner code in some cases.
        if (attributes == null || attributes.isEmpty()) {
            return this;
        }
        throwIfActionIsRetry();
        outputAttributes.putAll(attributes);
        return this;
    }

    private void throwIfActionIsRetry() {
        if (action == ResultAction.RETRY) {
            throw new IllegalArgumentException("Attributes cannot be added to Retry step results.");
        }
    }

    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(outputAttributes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StepResult that = (StepResult) obj;

        if (action != that.action) {
            return false;
        }
        if (!Objects.equals(resultCode, that.resultCode)) {
            return false;
        }
        if (!Objects.equals(message, that.message)) {
            return false;
        }

        return outputAttributes.equals(that.outputAttributes);

    }

    @Override
    public int hashCode() {
        int result = action.hashCode();
        result = 31 * result + (resultCode != null ? resultCode.hashCode() : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + (cause != null ? cause.hashCode() : 0);
        result = 31 * result + outputAttributes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StepResult{"
               + "action=" + action
               + ", resultCode='" + resultCode + '\''
               + ", message='" + message + '\''
               + ", outputAttributes=" + outputAttributes
               + '}';
    }
}
