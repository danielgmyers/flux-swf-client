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

package com.danielgmyers.flux.clients.swf.poller;

import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;

/**
 * Utility class to allow consistent naming for workflows and workflow steps.
 */
public final class TaskNaming {

    public static final String PARTITION_METADATA_MARKER_NAME_PREFIX = "_partitionMetadata";

    private TaskNaming() {}

    public static String workflowName(Workflow workflow) {
        return workflowName(workflow.getClass());
    }

    public static String workflowName(Class<? extends Workflow> clazz) {
        return clazz.getSimpleName();
    }

    public static String stepName(WorkflowStep step) {
        return stepName(step.getClass());
    }

    public static String stepName(Class<? extends WorkflowStep> stepClass) {
        return stepClass.getSimpleName();
    }

    public static String activityName(Workflow workflow, WorkflowStep step) {
        return activityName(workflowName(workflow), stepName(step));
    }

    public static String activityName(Workflow workflow, Class<? extends WorkflowStep> stepClass) {
        return activityName(workflowName(workflow), stepName(stepClass));
    }

    public static String activityName(String workflowName, WorkflowStep step) {
        return activityName(workflowName, stepName(step));
    }

    public static String activityName(Class<? extends Workflow> workflowClass, Class<? extends WorkflowStep> stepClass) {
        return activityName(workflowName(workflowClass), stepName(stepClass));
    }

    public static String activityName(String workflowName, String stepName) {
        return String.format("%s.%s", workflowName, stepName);
    }

    /**
     * Given a full activity name (as produced by {@link #activityName}), extracts the workflow name.
     */
    public static String workflowNameFromActivityName(String activityName) {
        String[] parts = activityName.split("\\.");
        if (parts.length != 2) {
            throw new RuntimeException("Invalid activity name: " + activityName);
        }
        return parts[0];
    }

    /**
     * Given a full activity name (as produced by {@link #activityName}), extracts the step name.
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

    /**
     * Given a marker name (as produced by {@link #partitionMetadataMarkerName},
     * determines whether it is a partition metadata marker.
     */
    public static boolean isPartitionMetadataMarker(String markerName) {
        return markerName != null && markerName.startsWith(PARTITION_METADATA_MARKER_NAME_PREFIX);
    }

    /**
     * Given a step name and information about how many metadata markers there are,
     * constructs a partition metadata marker name.
     */
    public static String partitionMetadataMarkerName(String stepName, long subsetId, long markerCount) {
        return String.format("%s.%s.%d.%d", PARTITION_METADATA_MARKER_NAME_PREFIX, stepName, subsetId, markerCount);
    }

    /**
     * Given a full partition metadata marker name (as produced by {@link #activityName}),
     * extracts the step name.
     */
    public static String extractPartitionMetadataMarkerStepName(String metadataMarkerName) {
        String[] parts = metadataMarkerName.split("\\.");
        if (parts.length != 4) {
            throw new RuntimeException("Invalid metadata marker name: " + metadataMarkerName);
        }
        return parts[1];
    }

    /**
     * Given a full partition metadata marker name (as produced by {@link #activityName}),
     * extracts the marker subset id number.
     */
    public static Long extractPartitionMetadataMarkerSubsetId(String metadataMarkerName) {
        String[] parts = metadataMarkerName.split("\\.");
        if (parts.length != 4) {
            throw new RuntimeException("Invalid metadata marker name: " + metadataMarkerName);
        }
        return Long.parseLong(parts[2]);
    }

    /**
     * Given a full partition metadata marker name (as produced by {@link #activityName}),
     * extracts the marker count.
     */
    public static Long extractPartitionMetadataMarkerCount(String metadataMarkerName) {
        String[] parts = metadataMarkerName.split("\\.");
        if (parts.length != 4) {
            throw new RuntimeException("Invalid metadata marker name: " + metadataMarkerName);
        }
        return Long.parseLong(parts[3]);
    }
}
