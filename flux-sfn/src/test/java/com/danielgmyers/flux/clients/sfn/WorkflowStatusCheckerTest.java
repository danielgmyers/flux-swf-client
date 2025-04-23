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

import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.testutil.ManualClock;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.WorkflowInfo;
import com.danielgmyers.flux.wf.WorkflowStatus;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionDoesNotExistException;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;

public class WorkflowStatusCheckerTest {
    private static class TestWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            return null; // the contents of this class don't matter for these tests
        }
    }

    private ManualClock clock;

    private static final String REGION = "us-west-2";
    private static final String ACCOUNT_ID = "123456789012";
    private static final String MACHINE_ARN = SfnArnFormatter.workflowArn(REGION, ACCOUNT_ID, TestWorkflow.class);
    private static final String EXECUTION_ID = "abcdefg";
    private static final String EXECUTION_ARN = SfnArnFormatter.executionArn(REGION, ACCOUNT_ID, TestWorkflow.class, EXECUTION_ID);

    private IMocksControl mockery;
    private SfnClient sfn;
    private WorkflowStatusChecker wsc;

    @BeforeEach
    public void setup() {
        clock = new ManualClock(Instant.now());
        mockery = EasyMock.createControl();
        sfn = mockery.createMock(SfnClient.class);
        wsc = new WorkflowStatusCheckerImpl(clock, sfn, EXECUTION_ARN);
    }

    @Test
    public void testWorkflowInfoValidWhenUnableToReachSfn() {
        EasyMock.expect(sfn.describeExecution(buildDescribeRequest())).andThrow(ExecutionDoesNotExistException.builder().build()).times(2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.UNKNOWN, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.UNKNOWN);

        mockery.verify();
    }

    @Test
    public void testWorkflowUnknownWhenWorkflowExecutionNotFound() {
        EasyMock.expect(sfn.describeExecution(buildDescribeRequest())).andThrow(ExecutionDoesNotExistException.builder().build()).times(2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.UNKNOWN, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.UNKNOWN);

        mockery.verify();
    }

    @Test
    public void testWorkflowInProgress() {
        expectDescribeStatus(ExecutionStatus.RUNNING, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.IN_PROGRESS, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.IN_PROGRESS);

        mockery.verify();
    }

    @Test
    public void testWorkflowPendingRedrive() {
        expectDescribeStatus(ExecutionStatus.PENDING_REDRIVE, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.IN_PROGRESS, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.IN_PROGRESS);

        mockery.verify();
    }

    @Test
    public void testWorkflowCompleted() {
        expectDescribeStatus(ExecutionStatus.SUCCEEDED, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.COMPLETED, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.COMPLETED);

        mockery.verify();
    }

    @Test
    public void testWorkflowFailed() {
        expectDescribeStatus(ExecutionStatus.FAILED, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.FAILED, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.FAILED);

        mockery.verify();
    }

    @Test
    public void testWorkflowTimedOut() {
        expectDescribeStatus(ExecutionStatus.TIMED_OUT, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.TIMED_OUT, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.TIMED_OUT);

        mockery.verify();
    }

    @Test
    public void testWorkflowTerminated() {
        expectDescribeStatus(ExecutionStatus.ABORTED, 2);
        mockery.replay();

        Assertions.assertEquals(WorkflowStatus.TERMINATED, wsc.checkStatus());
        validateWorkflowInfo(wsc.getWorkflowInfo(), WorkflowStatus.TERMINATED);

        mockery.verify();
    }

    private DescribeExecutionRequest buildDescribeRequest() {
        return DescribeExecutionRequest.builder()
                .executionArn(EXECUTION_ARN)
                .build();
    }

    private void expectDescribeStatus(ExecutionStatus executionStatus, int times) {
         DescribeExecutionResponse response = DescribeExecutionResponse.builder()
                 .executionArn(EXECUTION_ARN)
                 .stateMachineArn(MACHINE_ARN)
                 .name(EXECUTION_ID)
                 .status(executionStatus)
                 .build();

        EasyMock.expect(sfn.describeExecution(buildDescribeRequest())).andReturn(response).times(times);
    }

    private void validateWorkflowInfo(WorkflowInfo workflowInfo, WorkflowStatus expectedStatus) {
        Assertions.assertEquals(expectedStatus, workflowInfo.getWorkflowStatus());
        Assertions.assertEquals(clock.instant(), workflowInfo.getSnapshotTime());
        Assertions.assertEquals(EXECUTION_ID, workflowInfo.getWorkflowId());
        Assertions.assertEquals(EXECUTION_ARN, workflowInfo.getExecutionId());
    }
}
