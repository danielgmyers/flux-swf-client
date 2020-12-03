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

package software.amazon.aws.clients.swf.flux;

import java.util.Map;

import software.amazon.aws.clients.swf.flux.wf.Workflow;

/**
 * Represents an object that can execute workflows against a different endpoint and/or using different credentials
 * than the main FluxCapacitor. Should be used only when there is no cleaner way to do cross-region or
 * cross-acount operations (e.g. when there are no API endpoints available other than the SWF endpoints).
 *
 * Some caveats: this assumes you're already running the real FluxCapacitor against the remote region in the provided account,
 * which should take care of registering workflows, domains, etc.
 */
public interface RemoteWorkflowExecutor {
    /**
     * Executes a workflow of the specified type, using the specified workflow ID and input map,
     * using the region and credentials that were configured on the RemoteWorkflowExecutor when it was created.
     *
     * Returns a WorkflowStatusChecker object which can be used to monitor the status of the requested workflow.
     *
     * @param workflowType The class of the workflow which should be run
     * @param workflowId A unique identifier for the workflow execution (SWF dedupes on this)
     * @param workflowInput The map of input values for the workflow execution
     */
    WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType,
                                          String workflowId, Map<String, Object> workflowInput);
}
