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

package software.amazon.aws.clients.swf.flux.testutil;

import software.amazon.aws.clients.swf.flux.wf.Workflow;

/**
 * Utility class for StubFluxCapacitor to keep track of workflows which have been executed.
 */
class WorkflowExecutionRecord {

    private final Class<? extends Workflow> workflowType;
    private final String workflowId;

    public WorkflowExecutionRecord(Class<? extends Workflow> workflowType, String workflowId) {
        this.workflowType = workflowType;
        this.workflowId = workflowId;
    }

    public Class<? extends Workflow> getWorkflowType() {
        return workflowType;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        WorkflowExecutionRecord that = (WorkflowExecutionRecord) obj;

        if (!workflowType.equals(that.workflowType)) {
            return false;
        }
        return workflowId.equals(that.workflowId);

    }

    @Override
    public int hashCode() {
        int result = workflowType.hashCode();
        result = 31 * result + workflowId.hashCode();
        return result;
    }
}
