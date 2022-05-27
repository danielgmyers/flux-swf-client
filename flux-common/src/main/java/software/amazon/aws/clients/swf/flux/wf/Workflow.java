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

package software.amazon.aws.clients.swf.flux.wf;

import java.time.Duration;

import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;

/**
 * An interface defining the operations a workflow object must implement.
 */
public interface Workflow {

    String DEFAULT_TASK_LIST_NAME = "default";
    Duration WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT = Duration.ofDays(21);

    /**
     * Should return the WorkflowGraph representing the various paths the workflow may take through its steps.
     *
     * Implementations should build the WorkflowGraph object using WorkflowGraphBuilder.
     */
    WorkflowGraph getGraph();

    /**
     * Defines which task list should be used to execute workflows of this type.
     * Defaults to "default". More information on tasks lists can be found here:
     * http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-task-lists.html
     *
     * Flux allocates decider and activity threads for each task list you configure.
     *
     * It is dangerous to change this for an existing workflow -- make sure you have no active executions.
     */
    default String taskList() {
        return DEFAULT_TASK_LIST_NAME;
    }

    /**
     * Defines the maximum start-to-close duration for executions of this workflow.
     * Executions that last longer than this duration will be terminated by SWF.
     *
     * Maximum duration allowed by SWF is 1 year (parameter executionStartToCloseTimeout):
     * http://docs.aws.amazon.com/amazonswf/latest/apireference/API_StartWorkflowExecution.html
     *
     * This duration is applied per workflow execution. Specifying an invalid value here will result in an exception from
     * FluxCapacitor.executeWorkflow when it asks SWF to start the workflow execution.
     *
     * Defaults to 21 days.
     *
     * For best results, if a workflow can legitimately run for more than 21 days, the workflow should instead
     * be split into multiple distinct workflows that each take less than 21 days. Only extend this duration
     * if splitting the workflow in this manner is not possible.
     */
    default Duration maxStartToCloseDuration() {
        return WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT;
    }
}
