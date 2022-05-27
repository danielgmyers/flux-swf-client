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

package software.amazon.aws.clients.swf.flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.DescribeWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.ExecutionStatus;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;

/**
 * Implements the WorkflowStatusChecker interface.
 */
public class WorkflowStatusCheckerImpl implements WorkflowStatusChecker {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStatusCheckerImpl.class);

    private final SwfClient swf;
    private final String workflowDomain;
    private final String workflowId;
    private final String runId;

    /**
     * Constructs a WorkflowStatusCheckerImpl object. Package-private since it should only be created
     * by internal Flux code.
     */
    WorkflowStatusCheckerImpl(SwfClient swf, String workflowDomain, String workflowId, String runId) {
        this.swf = swf;
        this.workflowDomain = workflowDomain;
        this.workflowId = workflowId;
        this.runId = runId;
    }

    @Override
    public WorkflowStatus checkStatus() {
        WorkflowExecution execution = WorkflowExecution.builder().workflowId(workflowId).runId(runId).build();

        DescribeWorkflowExecutionRequest request
                = DescribeWorkflowExecutionRequest.builder()
                                                    .domain(workflowDomain)
                                                    .execution(execution)
                                                    .build();

        try {
            DescribeWorkflowExecutionResponse detail = swf.describeWorkflowExecution(request);
            // first check if it's still running
            if (ExecutionStatus.OPEN == detail.executionInfo().executionStatus()) {
                return WorkflowStatus.IN_PROGRESS;
            }

            switch (detail.executionInfo().closeStatus()) {
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
        } catch (Exception e) {
            log.info("Error retrieving workflow status", e);
        }

        log.info("Unable to determine workflow status for remote workflow (id: {}, run id: {})", workflowId, runId);

        return WorkflowStatus.UNKNOWN;
    }

    @Override
    public SwfClient getSwfClient() {
        return swf;
    }
}
