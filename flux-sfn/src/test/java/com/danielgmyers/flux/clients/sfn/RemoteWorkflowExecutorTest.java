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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.ex.WorkflowExecutionException;
import com.danielgmyers.flux.testutil.ManualClock;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ExecutionAlreadyExistsException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

public class RemoteWorkflowExecutorTest {

    static class TestWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            return null; // the implementation doesn't matter for these tests.
        }
    }

    static class TestUnregisteredWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            return null; // the implementation doesn't matter for these tests.
        }
    }

    private ManualClock clock;
    private IMocksControl mockery;
    private SfnClient sfn;
    private RemoteWorkflowExecutorImpl rwe;

    private Workflow workflow;
    private FluxCapacitorConfig fluxCapacitorConfig;
    private RemoteSfnClientConfig config;

    @BeforeEach
    public void setup() {
        clock = new ManualClock(Instant.now());

        workflow = new TestWorkflow();

        mockery = EasyMock.createControl();
        sfn = mockery.createMock(SfnClient.class);

        Map<Class<? extends Workflow>, Workflow> workflowsByClass = new HashMap<>();
        workflowsByClass.put(TestWorkflow.class, workflow);

        fluxCapacitorConfig = new FluxCapacitorConfig();
        fluxCapacitorConfig.setAwsRegion("us-west-2");
        fluxCapacitorConfig.setAwsAccountId("123456789012");

        config = new RemoteSfnClientConfig();
        config.setAwsRegion("us-east-2");
        config.setAwsAccountId("987654321098");
        rwe = new RemoteWorkflowExecutorImpl(clock, new NoopMetricRecorderFactory(), workflowsByClass, sfn, fluxCapacitorConfig, config);
    }

    @Test
    public void testExecuteWorkflow() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, config.getAwsAccountId(), workflowId, input);

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_IgnoresExecutionTags() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, config.getAwsAccountId(), workflowId, input);

        mockery.replay();
        // execution tags are not supported by Step Functions so we just log a warning and ignore them.
        Set<String> executionTags = new HashSet<>();
        executionTags.add("asdf");
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input, executionTags);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_usesFluxCapacitorAccountIfNotInRemoteConfig() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        config.setAwsAccountId(null);

        expectStartWorkflowRequest(workflow, fluxCapacitorConfig.getAwsAccountId(), workflowId, input);

        mockery.replay();
        rwe.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_workflowAlreadyStarted() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        expectStartWorkflowRequest(workflow, config.getAwsAccountId(), workflowId, input,
                                   ExecutionAlreadyExistsException.builder().build());

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
            rwe.executeWorkflow(TestUnregisteredWorkflow.class, workflowId, input);
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

        expectStartWorkflowRequest(workflow, config.getAwsAccountId(), workflowId, input, new IllegalStateException("some-error"));

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

    private void expectStartWorkflowRequest(Workflow workflow, String expectedAccountId, String workflowId, Map<String, Object> input) {
        expectStartWorkflowRequest(workflow, expectedAccountId, workflowId, input, null);
    }

    private void expectStartWorkflowRequest(Workflow workflow, String expectedAccountId, String workflowId,
                                            Map<String, Object> input, Exception exceptionToThrow) {
        StartExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(workflow, config.getAwsRegion(), expectedAccountId,
                                                              workflowId, input);
        if (exceptionToThrow == null) {
            String executionArn = SfnArnFormatter.executionArn(config.getAwsRegion(), expectedAccountId, workflow.getClass(), "run-id");
            StartExecutionResponse workflowRun = StartExecutionResponse.builder().executionArn(executionArn).build();
            EasyMock.expect(sfn.startExecution(start)).andReturn(workflowRun);
        } else {
            EasyMock.expect(sfn.startExecution(start)).andThrow(exceptionToThrow);
        }
    }
}
