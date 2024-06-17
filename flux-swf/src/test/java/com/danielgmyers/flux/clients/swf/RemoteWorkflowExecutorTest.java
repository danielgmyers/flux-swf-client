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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowWithPartitionedStep;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.testutil.ManualClock;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionAlreadyStartedException;

public class RemoteWorkflowExecutorTest {

    private ManualClock clock;
    private IMocksControl mockery;
    private SwfClient swf;
    private RemoteWorkflowExecutorImpl rwe;

    private Workflow workflow;
    private RemoteSwfClientConfig config;

    @BeforeEach
    public void setup() {
        clock = new ManualClock(Instant.now());

        workflow = new TestWorkflow();

        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        Map<String, Workflow> workflowsByName = new HashMap<>();
        workflowsByName.put(TaskNaming.workflowName(workflow), workflow);

        config = new RemoteSwfClientConfig();
        config.setWorkflowDomain("test");

        rwe = new RemoteWorkflowExecutorImpl(clock, new NoopMetricRecorderFactory(), workflowsByName, swf, config);
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
            Assertions.fail();
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
            Assertions.fail();
        } catch(WorkflowExecutionException e) {
            // expected
            // make sure the cause was wrapped properly
            Assertions.assertEquals(IllegalStateException.class, e.getCause().getClass());
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
                = FluxCapacitorImpl.buildStartWorkflowRequest(config.getWorkflowDomain(), TaskNaming.workflowName(workflow),
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
