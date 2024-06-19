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

import java.time.Instant;
import java.util.Collections;
import java.util.Set;

import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;

import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;

public class SfnWorkflowInfo implements WorkflowInfo {

    private final Instant snapshotTime;
    private final String workflowId;
    private final String executionArn;
    private final WorkflowStatus workflowStatus;

    public SfnWorkflowInfo(Instant snapshotTime, DescribeExecutionResponse snapshot) {
        this(snapshotTime, snapshot.name(), snapshot.executionArn(), executionStatusToWorkflowStatus(snapshot.status()));
    }

    public SfnWorkflowInfo(Instant snapshotTime, String workflowId, String executionArn, WorkflowStatus status) {
        this.snapshotTime = snapshotTime;
        this.workflowId = workflowId;
        this.executionArn = executionArn;
        this.workflowStatus = status;
    }

    @Override
    public Instant getSnapshotTime() {
        return snapshotTime;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    @Override
    public String getExecutionId() {
        return executionArn;
    }

    @Override
    public WorkflowStatus getWorkflowStatus() {
        return workflowStatus;
    }

    private static WorkflowStatus executionStatusToWorkflowStatus(ExecutionStatus executionStatus) {
        switch (executionStatus) {
            case RUNNING:
            case PENDING_REDRIVE:
                return WorkflowStatus.IN_PROGRESS;
            case SUCCEEDED:
                return WorkflowStatus.COMPLETED;
            case FAILED:
                return WorkflowStatus.FAILED;
            case TIMED_OUT:
                return WorkflowStatus.TIMED_OUT;
            case ABORTED:
                return WorkflowStatus.TERMINATED;
            case UNKNOWN_TO_SDK_VERSION:
            default:
                return WorkflowStatus.UNKNOWN;
        }
    }

    /**
     * Step Functions doesn't natively support tags on executions.
     */
    @Override
    public Set<String> getExecutionTags() {
        return Collections.emptySet();
    }
}
