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

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestBranchingWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPeriodicWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowCustomStartToCloseDuration;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflowCustomTaskList;
import com.danielgmyers.flux.ex.WorkflowExecutionException;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.ActivityTypeInfo;
import software.amazon.awssdk.services.swf.model.DescribeDomainRequest;
import software.amazon.awssdk.services.swf.model.DescribeDomainResponse;
import software.amazon.awssdk.services.swf.model.ListActivityTypesRequest;
import software.amazon.awssdk.services.swf.model.ListActivityTypesResponse;
import software.amazon.awssdk.services.swf.model.ListWorkflowTypesRequest;
import software.amazon.awssdk.services.swf.model.ListWorkflowTypesResponse;
import software.amazon.awssdk.services.swf.model.RegisterActivityTypeRequest;
import software.amazon.awssdk.services.swf.model.RegisterActivityTypeResponse;
import software.amazon.awssdk.services.swf.model.RegisterDomainRequest;
import software.amazon.awssdk.services.swf.model.RegisterDomainResponse;
import software.amazon.awssdk.services.swf.model.RegisterWorkflowTypeRequest;
import software.amazon.awssdk.services.swf.model.RegisterWorkflowTypeResponse;
import software.amazon.awssdk.services.swf.model.RegistrationStatus;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionAlreadyStartedException;
import software.amazon.awssdk.services.swf.model.WorkflowType;
import software.amazon.awssdk.services.swf.model.WorkflowTypeInfo;
import software.amazon.awssdk.services.swf.paginators.ListActivityTypesIterable;
import software.amazon.awssdk.services.swf.paginators.ListWorkflowTypesIterable;

public class FluxCapacitorTest {

    private static final String DOMAIN = "test";

    private IMocksControl mockery;
    private SwfClient swf;

    private Workflow workflow;
    private String workflowName;
    private FluxCapacitorImpl fc;
    private FluxCapacitorConfig config;

    @BeforeEach
    public void setup() {
        workflow = new TestWorkflow();
        workflowName = TaskNaming.workflowName(workflow);

        mockery = EasyMock.createControl();
        swf = mockery.createMock(SwfClient.class);

        config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        fc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());
        fc.populateNameMaps(Collections.singletonList(workflow));
    }

    @Test
    public void testExecuteWorkflow_happyCase() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN, workflowName, workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input, Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME));
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fc.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_happyCase_includesCustomExecutionTags() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        Set<String> customExecutionTags = new HashSet<>();
        customExecutionTags.add("kirk");
        customExecutionTags.add("spock");
        customExecutionTags.add("mccoy");

        Set<String> actualExecutionTags = new HashSet<>(customExecutionTags);
        actualExecutionTags.add(Workflow.DEFAULT_TASK_LIST_NAME);

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN, workflowName, workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input, actualExecutionTags);
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fc.executeWorkflow(TestWorkflow.class, workflowId, input, customExecutionTags);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_happyCase_includesCustomExecutionTags_excludesTaskListExecutionTagIfConfigDisabled() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        config.setAutomaticallyTagExecutionsWithTaskList(false);

        Set<String> customExecutionTags = new HashSet<>();
        customExecutionTags.add("kirk");
        customExecutionTags.add("spock");
        customExecutionTags.add("mccoy");

        Set<String> actualExecutionTags = new HashSet<>(customExecutionTags);

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN, workflowName, workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input, actualExecutionTags);
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fc.executeWorkflow(TestWorkflow.class, workflowId, input, customExecutionTags);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_usesTaskListNameFromWorkflow() {
        Workflow workflowCustomTaskList = new TestWorkflowCustomTaskList();
        String workflowCustomTaskListName = TaskNaming.workflowName(workflowCustomTaskList);

        Assertions.assertNotEquals(Workflow.DEFAULT_TASK_LIST_NAME, workflowCustomTaskList.taskList());

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        FluxCapacitorImpl fcCustom = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());
        fcCustom.populateNameMaps(Collections.singletonList(workflowCustomTaskList));

        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowCustomTaskListName,
                                                              workflowId,
                                                              workflowCustomTaskList.taskList(),
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              Collections.singleton(workflowCustomTaskList.taskList()));
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fcCustom.executeWorkflow(TestWorkflowCustomTaskList.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_usesTaskListNameFromWorkflow_excludesExecutionTagIfConfigDisabled() {
        Workflow workflowCustomTaskList = new TestWorkflowCustomTaskList();
        String workflowCustomTaskListName = TaskNaming.workflowName(workflowCustomTaskList);

        Assertions.assertNotEquals(Workflow.DEFAULT_TASK_LIST_NAME, workflowCustomTaskList.taskList());

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        config.setAutomaticallyTagExecutionsWithTaskList(false);
        FluxCapacitorImpl fcCustom = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());
        fcCustom.populateNameMaps(Collections.singletonList(workflowCustomTaskList));

        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowCustomTaskListName,
                                                              workflowId,
                                                              workflowCustomTaskList.taskList(),
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              Collections.emptySet());
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fcCustom.executeWorkflow(TestWorkflowCustomTaskList.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_usesBucketCountFromConfig() {
        Workflow workflowCustomTaskList = new TestWorkflowCustomTaskList();
        String workflowCustomTaskListName = TaskNaming.workflowName(workflowCustomTaskList);

        Assertions.assertNotEquals(Workflow.DEFAULT_TASK_LIST_NAME, workflowCustomTaskList.taskList());

        TaskListConfig taskListConfig = new TaskListConfig();
        taskListConfig.setBucketCount(10);

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        config.putTaskListConfig(workflowCustomTaskList.taskList(), taskListConfig);
        FluxCapacitorImpl fcCustom = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());
        fcCustom.populateNameMaps(Collections.singletonList(workflowCustomTaskList));

        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        Capture<StartWorkflowExecutionRequest> capturedStart = EasyMock.newCapture();
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(EasyMock.capture(capturedStart))).andReturn(workflowRun);

        mockery.replay();
        fcCustom.executeWorkflow(TestWorkflowCustomTaskList.class, workflowId, input);

        Set<String> bucketedTaskListNames = new HashSet<>();
        for (int i = 1; i <= taskListConfig.getBucketCount(); i++) {
            bucketedTaskListNames.add(FluxCapacitorImpl.synthesizeBucketedTaskListName(workflowCustomTaskList.taskList(), i));
        }

        // strictly speaking, since bucket 1 is the original, number-less name, this doesn't prove we used the bucket count.
        // however since it's random, we can't fail the test if we didn't use a numbered bucket.
        String actualTaskListName = capturedStart.getValue().taskList().name();
        Assertions.assertTrue(bucketedTaskListNames.contains(actualTaskListName));

        // We always use the original task list name, not the bucketed name, for the execution tag
        Set<String> executionTags = Collections.singleton(workflowCustomTaskList.taskList());

        StartWorkflowExecutionRequest expectedStart
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowCustomTaskListName,
                                                              workflowId,
                                                              actualTaskListName,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              executionTags);
        Assertions.assertEquals(expectedStart, capturedStart.getValue());

        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_usesStartToCloseDurationFromWorkflow() {
        Workflow customWorkflow = new TestWorkflowCustomStartToCloseDuration();
        String customWorkflowName = TaskNaming.workflowName(customWorkflow);

        Assertions.assertNotEquals(Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT, customWorkflow.maxStartToCloseDuration());

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        FluxCapacitorImpl fcCustom = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());
        fcCustom.populateNameMaps(Collections.singletonList(customWorkflow));

        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              customWorkflowName,
                                                              workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              customWorkflow.maxStartToCloseDuration(),
                                                              input,
                                                              Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME));
        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fcCustom.executeWorkflow(TestWorkflowCustomStartToCloseDuration.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_includesInputParameters() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = new HashMap<>();
        input.put("someKey", "some value huzzah");
        input.put("aNumber", 7L);

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowName,
                                                              workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME));

        Map<String, String> decodedExpectedInput = new HashMap<>();
        for (Entry<String, ?> entry : input.entrySet()) {
            decodedExpectedInput.put(entry.getKey(), StepAttributes.encode(entry.getValue()));
        }
        Assertions.assertEquals(StepAttributes.encode(decodedExpectedInput), start.input());

        StartWorkflowExecutionResponse workflowRun = StartWorkflowExecutionResponse.builder().runId("run-id").build();
        EasyMock.expect(swf.startWorkflowExecution(start)).andReturn(workflowRun);

        mockery.replay();
        fc.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_workflowAlreadyStarted() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowName,
                                                              workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME));

        EasyMock.expect(swf.startWorkflowExecution(start)).andThrow(WorkflowExecutionAlreadyStartedException.builder().build());

        mockery.replay();
        fc.executeWorkflow(TestWorkflow.class, workflowId, input);
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_unrecognizedWorkflow() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        FluxCapacitorImpl fcNoWorkflows = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());

        fcNoWorkflows.populateNameMaps(List.of(new TestBranchingWorkflow()));

        mockery.replay();
        try {
            fcNoWorkflows.executeWorkflow(TestWorkflow.class, workflowId, input);
            Assertions.fail();
        } catch(WorkflowExecutionException e) {
            Assertions.assertTrue(e.getMessage().contains("not provided"));
            // expected
        }
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_notInitialized() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        FluxCapacitorConfig config  = new FluxCapacitorConfig();
        config.setSwfDomain(DOMAIN);
        FluxCapacitor fcNoWorkflows = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), swf, config, Clock.systemUTC());

        mockery.replay();
        try {
            fcNoWorkflows.executeWorkflow(TestWorkflow.class, workflowId, input);
            Assertions.fail();
        } catch(WorkflowExecutionException e) {
            Assertions.assertTrue(e.getMessage().contains("initialized"));
            // expected
        }
        mockery.verify();
    }

    @Test
    public void testExecuteWorkflow_someOtherWorkflowExecutionProblem() {
        String workflowId = "my-workflow-id";
        Map<String, Object> input = Collections.emptyMap();

        StartWorkflowExecutionRequest start
                = FluxCapacitorImpl.buildStartWorkflowRequest(DOMAIN,
                                                              workflowName,
                                                              workflowId,
                                                              Workflow.DEFAULT_TASK_LIST_NAME,
                                                              Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT,
                                                              input,
                                                              Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME));
        // the actual error doesn't matter, just needs to be identifiable
        EasyMock.expect(swf.startWorkflowExecution(start)).andThrow(new IllegalStateException("some-error"));

        mockery.replay();
        try {
            fc.executeWorkflow(TestWorkflow.class, workflowId, input);
            Assertions.fail();
        } catch(WorkflowExecutionException e) {
            // expected
            // make sure the cause was wrapped properly
            Assertions.assertEquals(IllegalStateException.class, e.getCause().getClass());
        }
        mockery.verify();
    }

    @Test
    public void registersDomainIfNotAlreadyRegistered() {
        EasyMock.expect(swf.describeDomain(DescribeDomainRequest.builder().name(DOMAIN).build()))
                .andThrow(UnknownResourceException.builder().build());

        RegisterDomainRequest register = RegisterDomainRequest.builder().name(DOMAIN)
                .workflowExecutionRetentionPeriodInDays(FluxCapacitorImpl.WORKFLOW_EXECUTION_RETENTION_PERIOD_IN_DAYS)
                .build();
        EasyMock.expect(swf.registerDomain(register)).andReturn(RegisterDomainResponse.builder().build());

        mockery.replay();
        fc.ensureDomainExists();
        mockery.verify();
    }

    @Test
    public void registersDomainIfNotAlreadyRegistered_HandlesThrottling() {
        EasyMock.expect(swf.describeDomain(DescribeDomainRequest.builder().name(DOMAIN).build()))
                .andThrow(SdkServiceException.builder().statusCode(429).build());
        EasyMock.expect(swf.describeDomain(DescribeDomainRequest.builder().name(DOMAIN).build()))
                .andThrow(UnknownResourceException.builder().build());

        RegisterDomainRequest register = RegisterDomainRequest.builder().name(DOMAIN)
                .workflowExecutionRetentionPeriodInDays(FluxCapacitorImpl.WORKFLOW_EXECUTION_RETENTION_PERIOD_IN_DAYS)
                .build();
        EasyMock.expect(swf.registerDomain(register)).andThrow(SdkServiceException.builder().statusCode(429).build());
        EasyMock.expect(swf.registerDomain(register)).andReturn(RegisterDomainResponse.builder().build());

        mockery.replay();
        fc.ensureDomainExists();
        mockery.verify();
    }

    @Test
    public void doesNotRegisterDomainIfAlreadyRegistered() {
        EasyMock.expect(swf.describeDomain(DescribeDomainRequest.builder().name(DOMAIN).build()))
                .andReturn(DescribeDomainResponse.builder().build()); // contents don't matter for now

        mockery.replay();
        fc.ensureDomainExists();
        mockery.verify();
    }

    @Test
    public void registersWorkflowIfNotAlreadyRegistered() {
        expectDescribeWorkflows(false);

        RegisterWorkflowTypeRequest register = FluxCapacitorImpl.buildRegisterWorkflowRequest(DOMAIN, workflowName);

        EasyMock.expect(swf.registerWorkflowType(register)).andReturn(RegisterWorkflowTypeResponse.builder().build());

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();
    }

    @Test
    public void registersWorkflowIfNotAlreadyRegistered_HandlesThrottling() {
        expectDescribeWorkflows(false);

        RegisterWorkflowTypeRequest register = FluxCapacitorImpl.buildRegisterWorkflowRequest(DOMAIN, workflowName);

        EasyMock.expect(swf.registerWorkflowType(register)).andThrow(SdkServiceException.builder().statusCode(429).build());
        EasyMock.expect(swf.registerWorkflowType(register)).andReturn(RegisterWorkflowTypeResponse.builder().build());

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();
    }

    @Test
    public void doesNotRegisterWorkflowIfAlreadyRegistered() {
        expectDescribeWorkflows(true);

        mockery.replay();
        fc.registerWorkflows();
        mockery.verify();
    }

    @Test
    public void registersActivitiesIfNotAlreadyRegistered() {
        expectDescribeActivities(false);

        for(WorkflowGraphNode node : workflow.getGraph().getNodes().values()) {
            String activityName = TaskNaming.activityName(workflowName, node.getStep());
            RegisterActivityTypeRequest register = FluxCapacitorImpl.buildRegisterActivityRequest(DOMAIN, activityName);

            EasyMock.expect(swf.registerActivityType(register)).andReturn(RegisterActivityTypeResponse.builder().build());
        }

        mockery.replay();
        fc.registerActivities();
        mockery.verify();
    }

    @Test
    public void registersActivitiesIfNotAlreadyRegistered_HandlesThrottling() {
        expectDescribeActivities(false);

        for(WorkflowGraphNode node : workflow.getGraph().getNodes().values()) {
            String activityName = TaskNaming.activityName(workflowName, node.getStep());
            RegisterActivityTypeRequest register = FluxCapacitorImpl.buildRegisterActivityRequest(DOMAIN, activityName);

            EasyMock.expect(swf.registerActivityType(register)).andThrow(SdkServiceException.builder().statusCode(429).build());
            EasyMock.expect(swf.registerActivityType(register)).andReturn(RegisterActivityTypeResponse.builder().build());
        }

        mockery.replay();
        fc.registerActivities();
        mockery.verify();
    }

    @Test
    public void doesNotRegisterActivitiesIfAlreadyRegistered() {
        expectDescribeActivities(true);

        mockery.replay();
        fc.registerActivities();
        mockery.verify();
    }

    @Test
    public void testHostnameShortener() {
        String base = "ec2-123-45-67-89.us-west-2";
        Assertions.assertEquals(base, FluxCapacitorImpl.shortenHostnameForIdentity(base + ".ec2.amazonaws.com"));
        Assertions.assertEquals(base, FluxCapacitorImpl.shortenHostnameForIdentity(base + ".ec2.amazonaws.com.cn"));
        Assertions.assertEquals(base, FluxCapacitorImpl.shortenHostnameForIdentity(base + ".compute.amazonaws.com"));
        Assertions.assertEquals(base, FluxCapacitorImpl.shortenHostnameForIdentity(base + ".compute.amazonaws.com.cn"));
        Assertions.assertEquals(base, FluxCapacitorImpl.shortenHostnameForIdentity(base + ".compute.internal"));
    }

    static <T extends SdkPojo> T sdkFieldMatcher(T request) {
        EasyMock.reportMatcher(new IArgumentMatcher() {
            @Override
            public boolean matches(Object o) {
                return request.equalsBySdkFields(o);
            }

            @Override
            public void appendTo(StringBuffer stringBuffer) {
                stringBuffer.append("request{" + request + "}");
            }
        });
        return request;
    }

    private void expectDescribeWorkflows(boolean shouldExist) {
        ListWorkflowTypesResponse response;
        if(shouldExist) {
            WorkflowType type = WorkflowType.builder().name(workflowName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build();
            WorkflowTypeInfo info = WorkflowTypeInfo.builder().creationDate(Instant.now())
                    .status(RegistrationStatus.REGISTERED).workflowType(type).build();
            response = ListWorkflowTypesResponse.builder().typeInfos(Collections.singletonList(info)).build();
        } else {
            response = ListWorkflowTypesResponse.builder().typeInfos(Collections.emptyList()).build();
        }

        ListWorkflowTypesRequest request = ListWorkflowTypesRequest.builder()
                .registrationStatus(RegistrationStatus.REGISTERED).domain(DOMAIN).build();
        EasyMock.expect(swf.listWorkflowTypes(sdkFieldMatcher(request))).andReturn(response); // called internally by the iterable
        EasyMock.expect(swf.listWorkflowTypesPaginator(request)).andReturn(new ListWorkflowTypesIterable(swf, request));
    }

    private void expectDescribeActivities(boolean shouldExist) {
        ListActivityTypesResponse response;
        if(shouldExist) {
            List<ActivityTypeInfo> infoList = new ArrayList<>();
            for(WorkflowGraphNode node : workflow.getGraph().getNodes().values()) {
                ActivityType type = ActivityType.builder().name(TaskNaming.activityName(workflowName, node.getStep()))
                        .version(FluxCapacitorImpl.WORKFLOW_VERSION).build();
                ActivityTypeInfo info = ActivityTypeInfo.builder().creationDate(Instant.now())
                        .status(RegistrationStatus.REGISTERED).activityType(type).build();
                infoList.add(info);
            }
            response = ListActivityTypesResponse.builder().typeInfos(infoList).build();
        } else {
            response = ListActivityTypesResponse.builder().typeInfos(Collections.emptyList()).build();
        }

        ListActivityTypesRequest request = ListActivityTypesRequest.builder()
                .registrationStatus(RegistrationStatus.REGISTERED).domain(DOMAIN).build();
        EasyMock.expect(swf.listActivityTypes(sdkFieldMatcher(request))).andReturn(response); // called internally by the iterable
        EasyMock.expect(swf.listActivityTypesPaginator(request)).andReturn(new ListActivityTypesIterable(swf, request));
    }

    @Test
    public void testIsPeriodic_FalseForNormalWorkflow() {
        Assertions.assertFalse(FluxCapacitorImpl.isPeriodicWorkflow(new TestWorkflow()));
    }

    @Test
    public void testIsPeriodic_TrueForPeriodicWorkflow() {
        Assertions.assertTrue(FluxCapacitorImpl.isPeriodicWorkflow(new TestPeriodicWorkflow()));
    }

    private static class InheritsPeriodicAnnotation extends TestPeriodicWorkflow {
        // intentionally empty, we just want to inherit the annotation
    }

    @Test
    public void testIsPeriodic_TrueForChildClassOfPeriodicWorkflowClass() {
        Assertions.assertTrue(FluxCapacitorImpl.isPeriodicWorkflow(new InheritsPeriodicAnnotation()));
    }
}
