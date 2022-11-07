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

package com.danielgmyers.flux.clients.swf;

import java.util.regex.Pattern;

import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;

/**
 * SWF has a service-side limit of 256 characters on the identifiers listed here unless otherwise noted.
 *
 * On startup, we register:
 * 1) Domains, which is fully user-specified via config. We don't join this with anything.
 * 2) WorkflowTypes, which we generate using the class names of the Workflow implementations.
 * 3) ActivityTypes, which we generate by appending the WorkflowStep class names to their corresponding Workflow class names,
 *    separated by a period. (This way, if a step is reused across workflows, it gets a different ActivityType for each workflow.)
 *
 * When polling for work, we use the current host's hostname as part of the poller's identity, along with an Flux-specified
 * poller type, a small integer (between 0 and the thread pool size), and the user-specified task list being polled.
 *
 * When polling for work and when starting a new workflow execution, we give SWF a user-specified task list name.
 *
 * When starting a new workflow execution, we give SWF a user-specified workflow ID, and (possibly) a list of tags.
 *
 * When choosing the next step to execute during a workflow execution, we generate an activity id by combining the
 * step name (without the workflow name), the retry number, and the partition ID (if applicable), joined with underscores:
 *   "MyStep_0_MyPartitionId"
 *
 * Retry attempts won't be larger than 4166 in practice because SWF has a limit of 25000 events in a workflow's event history,
 * and each attempt adds a minimum of six events to the event history. We will reserve four characters for the attempt count,
 * and two characters for the underscores.
 *
 * Partition IDs are user-specified arbitrary strings. We store the user-specified value in the "control" field of
 * the ScheduleActivityTaskDecision, which has a limit of 32768 characters, but for the activity ID we have two options:
 * 1) Use the user-specified partition ID directly, which means imposing character and length limits on the user.
 * 2) Hash the user-provided partition ID for the activity ID for uniqueness, so no limit is necessarily required.
 *    If we use SHA-256 and convert the result to a hex string, we would only need to reserve 64 characters in the activityId.
 *
 * However, we also store the list of partition IDs in the "details" field of the RecordMarkerDecision, which has a length
 * limit of 32768. We store other things in there as well. In practice, to keep the number of markers down to a minimum, we
 * should impose a stricter limit on partition ID length than 32768. A length limit of 256, if used at maximum, would result in
 * around 127 partition IDs per marker, but most users will not use anywhere near that maximum length for all partition IDs.
 *
 * Taken together, when a partition ID is provided, that means an activity ID is composed of:
 *   "WorkflowStepClassName_1234_f6dc4d73b6ed26e764babebca8c27b9d3eb880131f1499b6745bdf60b68505ce"
 * The most variable-length piece here is WorkflowStepClassName, so for activity ID purposes, its max length is 256-64-4-2 = 186.
 *
 * As noted above, however, workflow step class names are also used when registering ActivityTypes. Since that string has a
 * maximum length of 256, and we use one period as a delimiter, if we split it evenly we need to limit both
 * Workflow and WorkflowStep class implementations to a maximum class name length of 127 characters.
 *
 * While it is tempting to impose a smaller limit than that in order to discourage people from using excessively long
 * class names, we need to concern ourselves here only with values that SWF will reject or that will interfere with Flux.
 *
 * Hostnames and task list names are similarly joined together with other data to generate the poller identity. Specifically,
 * the identities look like this:
 *   "hostname_taskTypeName-taskListName_threadNum"
 * taskTypeName is a fixed string, either "decisionPoller" or "activityPoller", both of which are 14 characters long.
 * threadNum is an integer, in practice it will be significantly less than 1000, but we can reserve 4 characters.
 * If task list bucketing is enabled, then taskListName itself will be a composite of the user-specified task list name
 * and an internally-generated bucket number. For safety we should reserve 5 characters for the extra data.
 * This means we have (256 - 3 - 14 - 4 - 5) = 230 characters to split between hostname and the user's taskListName.
 *
 * We could split that evenly, but in practice hostnames are likely to be longer than task list names, so giving two thirds of the
 * available space (i.e. 154 characters) to the hostname seems appropriate.
 *
 * In summary, we will enforce these length limits:
 * * Domain name: 256
 * * Workflow class implementation name: 127
 * * WorkflowStep class implementation name: 127
 * * Hostname: 154
 * * Task list name: 76
 * * Workflow execution ID: 256
 * * Workflow execution tag: 256
 * * Partition ID: 256
 *
 * These fields will also validate the content of the string against SWF's list of allowed characters:
 * * Domain name
 * * Workflow class implementation name
 * * WorkflowStep class implementation name
 * * Task list name
 * * Workflow execution ID
 *
 * These fields allow any content within the prescribed length:
 * * Hostname
 * * Workflow execution tag
 * * Partition ID
 *
 * See also com.danielgmyers.flux.poller.TaskNaming.
 */
public final class IdentifierValidation {

    static final int MAX_DOMAIN_NAME_LENGTH = 256;
    static final int MAX_WORKFLOW_CLASS_NAME_LENGTH = 127;
    static final int MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH = 127;
    static final int MAX_HOSTNAME_LENGTH = 154;
    static final int MAX_TASK_LIST_NAME_LENGTH = 76;
    static final int MAX_WORKFLOW_EXECUTION_ID_LENGTH = 256;
    static final int MAX_WORKFLOW_EXECUTION_TAG_LENGTH = 256;
    static final int MAX_PARTITION_ID_LENGTH = 256;

    private static final Pattern INVALID_CHARACTERS = Pattern.compile("[:/|\u0000-\u001f\u007f-\u009f]+");

    private IdentifierValidation() {}

    public static void validateDomain(String domain) {
        validate("Domains", domain, MAX_DOMAIN_NAME_LENGTH, true);
    }

    public static void validateWorkflowName(Class<? extends Workflow> clazz) {
        validate("Workflow class names", TaskNaming.workflowName(clazz), MAX_WORKFLOW_CLASS_NAME_LENGTH, true);
    }

    public static void validateStepName(Class<? extends WorkflowStep> clazz) {
        validate("WorkflowStep class names", TaskNaming.stepName(clazz), MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH, true);
    }

    public static void validateHostname(String hostname) {
        validate("Hostnames", hostname, MAX_HOSTNAME_LENGTH, false);
    }

    public static void validateTaskListName(String taskListName) {
        validate("Task list names", taskListName, MAX_TASK_LIST_NAME_LENGTH, true);
    }

    public static void validateWorkflowExecutionId(String executionId) {
        validate("Workflow Execution IDs", executionId, MAX_WORKFLOW_EXECUTION_ID_LENGTH, true);
    }

    public static void validateWorkflowExecutionTag(String executionTag) {
        validate("Workflow Execution Tags", executionTag, MAX_WORKFLOW_EXECUTION_TAG_LENGTH, false);
    }

    public static void validatePartitionId(String partitionId) {
        validate("Partition IDs", partitionId, MAX_PARTITION_ID_LENGTH, false);
    }

    /**
     * For all the fields that limit input characters, SWF provides this description:
     *
     * The specified string must not contain a : (colon), / (slash), | (vertical bar),
     * or any control characters (\u0000-\u001f | \u007f-\u009f). Also, it must not be the literal string "arn".
     *
     * That last bit is weird, but the SWF API does in fact reject that input.
     *
     * Package-private for testing.
     */
    static void validate(String description, String inputToValidate, int max, boolean enforceCharacterConstraints) {
        if (inputToValidate == null || inputToValidate.isEmpty()) {
            throw new IllegalArgumentException(description + " must contain at least one character.");
        }
        if (inputToValidate.length() > max) {
            throw new IllegalArgumentException(description + " must not be longer than " + max + " characters.");
        }
        if (enforceCharacterConstraints) {
            if (INVALID_CHARACTERS.matcher(inputToValidate).find()) {
                throw new IllegalArgumentException(description + " must not contain not contain a : (colon), / (slash),"
                                       + " | (vertical bar), or any control characters (\\u0000-\\u001f | \\u007f-\\u009f).");
            } else if ("arn".equals(inputToValidate)) {
                throw new IllegalArgumentException(description + " must not be the literal string 'arn'.");
            }
        }
    }
}
