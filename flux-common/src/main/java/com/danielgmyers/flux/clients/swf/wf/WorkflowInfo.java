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

package com.danielgmyers.flux.clients.swf.wf;

import java.time.Instant;
import java.util.Set;

/**
 * Provides a point-in-time snapshot of information about a workflow execution.
 */
public interface WorkflowInfo {

    /**
     * Returns the time at which this snapshot was generated.
     */
    Instant getSnapshotTime();

    /**
     * Returns the user-specified workflow identifier, i.e. the value provided to FluxCapacitor.executeWorkflow().
     */
    String getWorkflowId();

    /**
     * Returns the system-generated identifier specific to this workflow execution.
     */
    String getExecutionId();

    /**
     * Returns the status of the workflow at this point in time.
     */
    WorkflowStatus getWorkflowStatus();

    /**
     * Returns the execution tags associated with this workflow execution.
     */
    Set<String> getExecutionTags();
}
