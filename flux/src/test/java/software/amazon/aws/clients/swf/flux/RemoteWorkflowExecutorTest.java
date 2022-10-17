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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    private IMocksControl mockery;
    private SwfClient swf;
    private RemoteWorkflowExecutorImpl rwe;

    private Workflow workflow;
    private FluxCapacitorConfig config;

    @Before
    public void setup() {
        workflow = new TestWorkflow();

        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        Map<String, Workflow> workflowsByName = new HashMap<>();
        workflowsByName.put(TaskNaming.workflowName(workflow), workflow);

        config = new FluxCapacitorConfig();
        config.setSwfDomain("test");

        rwe = new RemoteWorkflowExecutorImpl(new NoopMetricRecorderFactory(), workflowsByName, swf, config);
    }

    @Test
    public void testExecuteWorkflow() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        Set<String> executionTags = Collections.singleton(workflow.taskList());

        expectStartWorkflowRequest(workflow, workflowId, input, executionTags);

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_respectsAutoTagConfig() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        config.setAutomaticallyTagExecutionsWithTaskList(false);

        expectStartWorkflowRequest(workflow, workflowId, input, Collections.emptySet());

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_workflowAlreadyStarted() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        Set<String> executionTags = Collections.singleton(workflow.taskList());

        expectStartWorkflowRequest(workflow, workflowId, input, executionTags,
                                   WorkflowExecutionAlreadyStartedException.builder().build());

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

        Set<String> executionTags = Collections.singleton(workflow.taskList());

        expectStartWorkflowRequest(workflow, workflowId, input, executionTags, new IllegalStateException("some-error"));

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

    private void expectStartWorkflowRequest(Workflow workflow, String workflowId, Map<String, Object> input,
                                            Set<String> executionTags) {
        expectStartWorkflowRequest(workflow, workflowId, input, executionTags,null);
    }

    private void expectStartWorkflowRequest(Workflow workflow, String workflowId, Map<String, Object> input,
                                            Set<String> executionTags, Exception exceptionToThrow) {
        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(config.getSwfDomain(), TaskNaming.workflowName(workflow),
                                                              workflowId, workflow.taskList(),
                                                              workflow.maxStartToCloseDuration(), input, executionTags);
        if (exceptionToThrow == null) {
            StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
            EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);
        } else {
            EasyMock.expect(swf.startWorkflowExecution(start)).andThrow(exceptionToThrow);
        }
    }
}
