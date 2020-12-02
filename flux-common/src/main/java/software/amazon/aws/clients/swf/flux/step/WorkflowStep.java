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

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * An interface marking a class to be a workflow step.
 * The class must also define exactly one public method with the @StepApply annotation.
 */
public interface WorkflowStep {

    Duration ACTIVITY_TASK_HEARTBEAT_DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Attributes returned by this method are used by the WorkflowGraphBuilder to validate that all steps' input attributes
     * are made available by either the workflow input or at least one previous step in the workflow.
     * The keys of the map should be the attribute names; the values should be the type of the value returned by the step.
     * By default this returns an empty map.
     *
     * PartitionedWorkflowSteps can only output attributes from their @PartitionIdGenerator method.
     */
    default Map<String, Class<?>> declaredOutputAttributes() {
        return Collections.emptyMap();
    }

    /**
     * Defines the maximum time that a task can run before providing its progress
     * through the RecordActivityTaskHeartbeat action.
     * Failures to providing the progress through RecordActivityTaskHeartbeat will result in SWF
     * marking the activity execution as timed out.
     *
     * Maximum duration allowed by SWF is 1 year (parameter heartbeatTimeout):
     * http://docs.aws.amazon.com/amazonswf/latest/apireference/API_ScheduleActivityTaskDecisionAttributes.html
     *
     * This duration is applied per activity execution. Flux by default invokes RecordActivityTaskHeartbeat
     * every 10 seconds, so setting a value lower than that might cause activities to timeout erroneously.
     *
     * Defaults to 60 seconds.
     */
    default Duration activityTaskHeartbeatTimeout() {
        return ACTIVITY_TASK_HEARTBEAT_DEFAULT_TIMEOUT;
    }
}
