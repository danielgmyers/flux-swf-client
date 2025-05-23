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

import java.time.Clock;

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.util.ArnUtils;
import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;

/**
 * Implements the WorkflowStatusChecker interface.
 */
public class WorkflowStatusCheckerImpl implements WorkflowStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStatusCheckerImpl.class);

    private final Clock clock;
    private final SfnClient sfn;
    private final String executionArn;

    /**
     * Constructs a WorkflowStatusCheckerImpl object. Package-private since it should only be created
     * by internal Flux code.
     */
    WorkflowStatusCheckerImpl(Clock clock, SfnClient sfn, String executionArn) {
        this.clock = clock;
        this.sfn = sfn;
        this.executionArn = executionArn;
    }

    @Override
    public WorkflowInfo getWorkflowInfo() {
        DescribeExecutionRequest request = DescribeExecutionRequest.builder()
                .executionArn(executionArn).build();

        try {
            return new SfnWorkflowInfo(clock.instant(), sfn.describeExecution(request));
        } catch (Exception e) {
            log.info("Error retrieving workflow status", e);
        }

        log.info("Unable to determine workflow status for remote workflow {}", executionArn);
        return new SfnWorkflowInfo(clock.instant(), ArnUtils.extractQualifier(executionArn), executionArn, WorkflowStatus.UNKNOWN);
    }
}
