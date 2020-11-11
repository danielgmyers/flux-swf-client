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

package software.amazon.aws.clients.swf.flux.poller;

import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.wf.Workflow;

/**
 * Utility class to allow consistent naming for workflows and workflow steps.
 */
public final class TaskNaming {

    private TaskNaming() {}

    public static String workflowName(Workflow workflow) {
        return workflowName(workflow.getClass());
    }

    public static String workflowName(Class<? extends Workflow> clazz) {
        return clazz.getSimpleName();
    }

    public static String activityName(Workflow workflow, WorkflowStep step) {
        return String.format("%s.%s", workflowName(workflow), step.getClass().getSimpleName());
    }

    public static String activityName(Workflow workflow, Class<? extends WorkflowStep> stepClass) {
        return String.format("%s.%s", workflowName(workflow), stepClass.getSimpleName());
    }

    public static String activityName(String workflowName, WorkflowStep step) {
        return String.format("%s.%s", workflowName, step.getClass().getSimpleName());
    }

    public static String activityName(Class<? extends Workflow> workflowClass, Class<? extends WorkflowStep> stepClass) {
        return activityName(workflowName(workflowClass), stepClass.getSimpleName());
    }

    public static String activityName(String workflowName, String stepName) {
        return String.format("%s.%s", workflowName, stepName);
    }

    /**
     * Given a full activity name (as produced by TaskNaming.activityName()), extracts the workflow name.
     */
    public static String workflowNameFromActivityName(String activityName) {
        String[] parts = activityName.split("\\.");
        if (parts.length != 2) {
            throw new RuntimeException("Invalid activity name: " + activityName);
        }
        return parts[0];
    }

    /**
     * Given a full activity name (as produced by TaskNaming.activityName()), extracts the step name.
     */
    public static String stepNameFromActivityName(String activityName) {
        String[] parts = activityName.split("\\.");
        if (parts.length != 2) {
            throw new RuntimeException("Invalid activity name: " + activityName);
        }
        return parts[1];

    }

    /**
     * Given a workflow step, retry attempt and a partitionId (may be null),
     * returns the activity Id that should be used.
     */
    public static String createActivityId(WorkflowStep step, long retryAttempt, String partitionId) {
        return createActivityId(step.getClass().getSimpleName(), retryAttempt, partitionId);
    }

    /**
     * Given a workflow step name, retry attempt and a partitionId (may be null),
     * returns the activity Id that should be used.
     */
    public static String createActivityId(String stepName, long retryAttempt, String partitionId) {
        if (partitionId == null) {
            return stepName + "_" + retryAttempt;
        } else {
            return stepName + "_" + retryAttempt + "_" + partitionId;
        }
    }
}
