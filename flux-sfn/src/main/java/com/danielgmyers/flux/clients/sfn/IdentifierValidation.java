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

package com.danielgmyers.flux.clients.sfn;

import java.util.regex.Pattern;

import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;

/**
 * Step Functions has a service-side limit of 80 characters on the identifiers listed here unless otherwise noted.
 *
 * On startup, we register:
 * 1) State Machines, which we generate using the class names of the Workflow implementations.
 * 2) Activities, which we generate by appending the WorkflowStep class names to their corresponding Workflow class names,
 *    separated by a dash. (This way, if a step is reused across workflows, it gets a different ActivityType for each workflow.)
 *
 * State machine names are generated using the underlying Workflow class name. However, activity names also use the Workflow name
 * as part of their name, so we need to limit workflow names by how long they can be when part of activity names.
 *
 * Activity names are limited to 80 characters, and that space is split between the Workflow and WorkflowStep class name,
 * with a dash character between them. If we split the remaining 79 characters evenly between Workflow and WorkflowStep names,
 * that allows the Workflow and WorkflowStep implementation class names to be 39 characters long.
 *
 * When polling for work, we use the current host's hostname as part of the poller's identity, along with a Flux-specified
 * small integer (between 0 and the poller thread pool size). While we could include the activity name in the poller identity,
 * polling is explicitly per-activity anyway so there's not much value in that. The poller thread pool should be only a few threads
 * since we don't need to poll with as many threads as we have active workers, but if we pessimistically reserve four characters
 * for a separator and the thread number, that leaves 77 characters for the hostname.
 *
 * Note also that we allow the user to provide a "hostname transformer" callback; this length restriction should be enforced _after_
 * executing that callback.
 *
 * When starting a new workflow execution, we give Step Functions a user-specified workflow ID.
 *
 * Partition IDs are user-specified arbitrary strings. They are stored in the input data for a partitioned step.
 * The input data has a maximum size of 256KB, but it will contain data other than just the partition IDs. To keep the size of the
 * input data down, we will enforce a maximum size of 256 bytes for the partition IDs, but in practice most users will remain
 * far below that.
 *
 * While it is tempting to impose a smaller limit than that in order to discourage people from using excessively long
 * identifiers, we need to concern ourselves here only with values that Step Functions will reject or that will interfere with Flux.
 *
 * In summary, we will enforce these length limits:
 * * Workflow class implementation name: 39
 * * WorkflowStep class implementation name: 39
 * * Hostname: 77
 * * Workflow execution ID: 80
 * * Partition ID: 256
 *
 * These fields will also validate the content of the string against SWF's list of allowed characters:
 * * Workflow class implementation name
 * * WorkflowStep class implementation name
 * * Workflow execution ID
 *
 * These fields allow any content within the prescribed length:
 * * Hostname
 * * Partition ID
 */
public final class IdentifierValidation {

    static final int MAX_WORKFLOW_CLASS_NAME_LENGTH = 39;
    static final int MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH = 39;
    static final int MAX_HOSTNAME_LENGTH = 77;
    static final int MAX_WORKFLOW_EXECUTION_ID_LENGTH = 80;
    static final int MAX_PARTITION_ID_LENGTH = 256;

    private static final Pattern INVALID_CHARACTERS
            = Pattern.compile("[\u0000-\u001F\u007F-\u009F \n\t<>{}\\[\\]?*\"#%\\\\^|~`$&,;:/]+");

    private IdentifierValidation() {}

    public static void validateWorkflowName(Class<? extends Workflow> clazz) {
        validate("Workflow class names", TaskNaming.workflowName(clazz), MAX_WORKFLOW_CLASS_NAME_LENGTH, true);
    }

    public static void validateStepName(Class<? extends WorkflowStep> clazz) {
        validate("WorkflowStep class names", TaskNaming.stepName(clazz), MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH, true);
    }

    public static void validateHostname(String hostname) {
        validate("Hostnames", hostname, MAX_HOSTNAME_LENGTH, false);
    }

    public static void validateWorkflowExecutionId(String executionId) {
        validate("Workflow Execution IDs", executionId, MAX_WORKFLOW_EXECUTION_ID_LENGTH, true);
    }

    public static void validatePartitionId(String partitionId) {
        validate("Partition IDs", partitionId, MAX_PARTITION_ID_LENGTH, false);
    }

    /**
     * For all the fields that limit input characters, Step Functions provides this description:
     *
     * A name must not contain:
     *     white space
     *     brackets < > { } [ ]
     *     wildcard characters ? *
     *     special characters " # % \ ^ | ~ ` $ & , ; : /
     *     control characters (U+0000-001F, U+007F-009F)
     * To enable logging with CloudWatch Logs, the name should only contain 0-9, A-Z, a-z, - and _.
     *
     * Flux won't enforce that the user's naming be CloudWatch Logs compatible, but Flux's naming won't prevent it either.
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
        if (enforceCharacterConstraints && INVALID_CHARACTERS.matcher(inputToValidate).find()) {
            throw new IllegalArgumentException(description + " must not contain not contain whitespace, a comma,"
                    + " any of <, >, {, }, [, ], ?, *, \", #, %, \\, ^, |, ~, `, $, &, ;, :, /,"
                    + " or any control characters (\\u0000-\\u001f | \\u007f-\\u009f).");
        }
    }
}
