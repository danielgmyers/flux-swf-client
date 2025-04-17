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
import java.util.Set;

import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;

public class SfnWorkflowInfo implements WorkflowInfo {

    private Instant snapshotTime;

    public SfnWorkflowInfo(Instant snapshotTime) {
        this.snapshotTime = snapshotTime;
    }

    @Override
    public Instant getSnapshotTime() {
        return snapshotTime;
    }

    @Override
    public String getWorkflowId() {
        return null;
    }

    @Override
    public String getExecutionId() {
        return null;
    }

    @Override
    public WorkflowStatus getWorkflowStatus() {
        return WorkflowStatus.UNKNOWN;
    }

    @Override
    public Set<String> getExecutionTags() {
        return null;
    }
}
