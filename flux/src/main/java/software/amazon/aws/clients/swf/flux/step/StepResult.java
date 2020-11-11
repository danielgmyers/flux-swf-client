/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A container object for passing around the results of a workflow step,
 * including whether it succeeded and any attributes that should be passed along to the next step.
 */
public final class StepResult {

    public static final String SUCCEED_RESULT_CODE = "_succeed";
    public static final String FAIL_RESULT_CODE = "_fail";
    public static final String ALWAYS_RESULT_CODE = "_always";

    public static final Set<String> VALID_PARTITIONED_STEP_RESULT_CODES
            = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SUCCEED_RESULT_CODE, FAIL_RESULT_CODE, ALWAYS_RESULT_CODE)));

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
     * @return A success result object with no message.
     */
    public static StepResult success() {
        return success(null);
    }

    /**
     * Creates
     * @return A success result object with a specific message.
     */
    public static StepResult success(String message) {
        return success(SUCCEED_RESULT_CODE, message);
    }

    /**
     * Creates
     * @return A success result object with a specific result code and message.
     */
    public static StepResult success(String resultCode, String message) {
        if (resultCode == null || resultCode.isEmpty()) {
            throw new IllegalArgumentException("Result codes cannot be blank.");
        }
        return new StepResult(ResultAction.COMPLETE, resultCode, message);
    }

    /**
     * @return A retry result object with no message.
     */
    public static StepResult retry() {
        return new StepResult(ResultAction.RETRY, null, null);
    }

    /**
     * @return A retry result object with a specific message.
     */
    public static StepResult retry(String message) {
        return new StepResult(ResultAction.RETRY, null, message);
    }

    /**
     * @return A retry result object caused by a particular exception.
     */
    public static StepResult retry(Throwable cause) {
        return new StepResult(cause);
    }

    /**
     * @return A failure result object with no message.
     */
    public static StepResult failure() {
        return failure(null);
    }

    /**
     * @return A failure result object with a specific message.
     */
    public static StepResult failure(String message) {
        return failure(FAIL_RESULT_CODE, message);
    }

    /**
     * @param resultCode The custom resultCode to include in the result
     * @param message The message to include in the result
     * @return A failure result object with a specific message.
     */
    public static StepResult failure(String resultCode, String message) {
        if (resultCode == null || resultCode.isEmpty()) {
            throw new IllegalArgumentException("Result codes cannot be blank.");
        }
        return new StepResult(ResultAction.COMPLETE, resultCode, message);
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
