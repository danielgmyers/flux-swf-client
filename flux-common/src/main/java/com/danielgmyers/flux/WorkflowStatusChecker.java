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

package com.danielgmyers.flux;

import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;

/**
 * Allows checking the status of a workflow execution. Obtained from a FluxCapacitor or RemoteWorkflowExecutor.
 */
public interface WorkflowStatusChecker {

    /**
     * Queries the workflow service to determine the status of the associated workflow execution.
     * Returns WorkflowStatus.UNKNOWN if the status could not be retrieved.
     */
    default WorkflowStatus checkStatus() {
        WorkflowInfo info = getWorkflowInfo();
        if (info == null) {
            return WorkflowStatus.UNKNOWN;
        }
        return info.getWorkflowStatus();
    }

    /**
     * Queries the workflow service to retrieve detailed information about the workflow execution.
     * May return null if the information could not be retrieved.
     */
    WorkflowInfo getWorkflowInfo();
}
