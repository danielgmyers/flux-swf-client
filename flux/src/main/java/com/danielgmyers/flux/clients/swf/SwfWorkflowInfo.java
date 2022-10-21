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

package com.danielgmyers.flux.clients.swf;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import com.danielgmyers.flux.clients.swf.wf.WorkflowInfo;
import com.danielgmyers.flux.clients.swf.wf.WorkflowStatus;

import software.amazon.awssdk.services.swf.model.ExecutionStatus;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

public class SwfWorkflowInfo implements WorkflowInfo {

    private Instant snapshotTime;
    private WorkflowExecutionInfo workflowExecutionInfo;

    public SwfWorkflowInfo(Instant snapshotTime, WorkflowExecutionInfo workflowExecutionInfo) {
        this.snapshotTime = snapshotTime;
        this.workflowExecutionInfo = workflowExecutionInfo;
    }

    @Override
    public Instant getSnapshotTime() {
        return snapshotTime;
    }

    @Override
    public String getWorkflowId() {
        return workflowExecutionInfo.execution().workflowId();
    }

    @Override
    public String getExecutionId() {
        return workflowExecutionInfo.execution().runId();
    }

    @Override
    public WorkflowStatus getWorkflowStatus() {
        if (ExecutionStatus.OPEN == workflowExecutionInfo.executionStatus()) {
            return WorkflowStatus.IN_PROGRESS;
        }
        switch (workflowExecutionInfo.closeStatus()) {
            case FAILED:
                return WorkflowStatus.FAILED;
            case CANCELED:
                return WorkflowStatus.CANCELED;
            case TERMINATED:
                return WorkflowStatus.TERMINATED;
            case TIMED_OUT:
                return WorkflowStatus.TIMED_OUT;
            case COMPLETED:
            case CONTINUED_AS_NEW:
            case UNKNOWN_TO_SDK_VERSION: // we'll treat this as completed since we don't know what else to do.
            default:
                return WorkflowStatus.COMPLETED;
        }
    }

    @Override
    public Set<String> getExecutionTags() {
        return new HashSet<>(workflowExecutionInfo.tagList());
    }
}
