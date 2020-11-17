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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.metrics.NoopMetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.poller.TaskNaming;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflow;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestWorkflowWithPartitionedStep;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionAlreadyStartedException;

public class RemoteWorkflowExecutorTest {

    private static final String DOMAIN = "test";

    private IMocksControl mockery;
    private SwfClient swf;
    private RemoteWorkflowExecutorImpl rwe;

    private Workflow workflow;

    @Before
    public void setup() {
        workflow = new TestWorkflow();

        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        Map<String, Workflow> workflowsByName = new HashMap<>();
        workflowsByName.put(TaskNaming.workflowName(workflow), workflow);

        rwe = new RemoteWorkflowExecutorImpl(new NoopMetricRecorderFactory(), workflowsByName, swf, DOMAIN);
    }

    @Test
    public void testExecuteWorkflow() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, workflowId, input);

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_workflowAlreadyStarted() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, workflowId, input, WorkflowExecutionAlreadyStartedException.builder().build());

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_unrecognizedWorkflow() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        mockery.replay();
        try {
            rwe.executeWorkflow(TestWorkflowWithPartitionedStep.class, workflowId, input);
            Assert.fail();
        } catch(WorkflowExecutionException e) {
            // expected
        }
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_someOtherWorkflowExecutionProblem() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, workflowId, input, new IllegalStateException("some-error"));

        mockery.replay();
        try {
            rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
            Assert.fail();
        } catch(WorkflowExecutionException e) {
            // expected
            // make sure the cause was wrapped properly
            Assert.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }
        mockery.verify();
    }

    private void expectStartWorkflowRequest(Workflow workflow, String workflowId, Map<String, Object> input) {
        expectStartWorkflowRequest(workflow, workflowId, input, null);
    }

    private void expectStartWorkflowRequest(Workflow workflow, String workflowId, Map<String, Object> input, Exception exceptionToThrow) {
        StartWorkflowExecutionRequest start = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN, TaskNaming.workflowName(workflow),
                                                                                          workflowId, workflow.taskList(),
                                                                                          workflow.maxStartToCloseDuration(), input);
        if (exceptionToThrow == null) {
            StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
            EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);
        } else {
            EasyMock.expect(swf.startWorkflowExecution(start)).andThrow(exceptionToThrow);
        }
    }
}
