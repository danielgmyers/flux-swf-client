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

package software.amazon.aws.clients.swf.flux.testutil;

import java.util.HashMap;
import java.util.Map;

import software.amazon.aws.clients.swf.flux.RemoteWorkflowExecutor;
import software.amazon.aws.clients.swf.flux.WorkflowStatusChecker;
import software.amazon.aws.clients.swf.flux.poller.TaskNaming;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.services.swf.SwfClient;

/**
 * Stub implementation of the RemoteWorkflowExecutor interface, useful for writing unit tests against Flux.
 */
public class StubRemoteWorkflowExecutor implements RemoteWorkflowExecutor {

    private final Map<WorkflowExecutionRecord, Map<String, Object>> executedWorkflows;

    /**
     * Constructs a StubRemoteWorkflowExecutor object.
     */
    public StubRemoteWorkflowExecutor() {
        this.executedWorkflows = new HashMap<>();
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        executedWorkflows.put(new WorkflowExecutionRecord(workflowType, workflowId), workflowInput);
        return new WorkflowStatusChecker() {
            @Override
            public WorkflowStatus checkStatus() {
                return WorkflowStatus.UNKNOWN;
            }

            public SwfClient getSwfClient() {
                return null;
            }
        };
    }

    /**
     * Utility method allowing unit tests to verify that a workflow was executed with the expected id and input.
     * @param workflowType       - The type of workflow that was executed
     * @param expectedWorkflowId - The id of the workflow that was executed
     * @param expectedInput      - The input to the workflow that was executed
     */
    public void verifyWorkflowWasStarted(Class<? extends Workflow> workflowType, String expectedWorkflowId,
                                         Map<String, Object> expectedInput) {
        WorkflowExecutionRecord execution = new WorkflowExecutionRecord(workflowType, expectedWorkflowId);
        if (!executedWorkflows.containsKey(execution)) {
            throw new RuntimeException(String.format("No %s execution request for %s was found.",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId));
        }
        if (expectedInput == null && !executedWorkflows.get(execution).isEmpty()) {
            throw new RuntimeException(String.format("%s execution %s should have had empty input but instead had %s",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId,
                                                     StepAttributes.encode(executedWorkflows.get(execution))));
        }
        if (!executedWorkflows.get(execution).equals(expectedInput)) {
            throw new RuntimeException(String.format("%s execution %s should have had input %s but instead had %s",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId,
                                                     StepAttributes.encode(expectedInput),
                                                     StepAttributes.encode(executedWorkflows.get(execution))));
        }
    }

    /**
     * Utility method allowing unit tests to verify that a workflow was *not* executed with the specified id.
     * @param workflowType - The type of workflow that should not have been executed
     * @param workflowId   - The id of the workflow that should not have been executed
     */
    public void verifyWorkflowWasNotStarted(Class<? extends Workflow> workflowType, String workflowId) {
        WorkflowExecutionRecord execution = new WorkflowExecutionRecord(workflowType, workflowId);
        if (executedWorkflows.containsKey(execution)) {
            throw new RuntimeException(String.format("%s execution %s should not have been started.",
                                                     workflowType.getSimpleName(), workflowId));
        }
    }

    /**
     * Returns the number of unique workflows that were executed.
     * @return The count
     */
    public int countExecutedWorkflows() {
        return executedWorkflows.size();
    }

    /**
     * Resets the executed workflow cache, useful for preventing tests from interfering with each other.
     */
    public void resetExecutionCache() {
        executedWorkflows.clear();
    }
}
