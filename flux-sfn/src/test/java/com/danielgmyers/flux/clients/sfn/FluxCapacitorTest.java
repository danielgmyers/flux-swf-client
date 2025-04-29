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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ActivityListItem;
import software.amazon.awssdk.services.sfn.model.CreateActivityRequest;
import software.amazon.awssdk.services.sfn.model.CreateActivityResponse;
import software.amazon.awssdk.services.sfn.model.ListActivitiesRequest;
import software.amazon.awssdk.services.sfn.model.ListActivitiesResponse;
import software.amazon.awssdk.services.sfn.paginators.ListActivitiesIterable;

public class FluxCapacitorTest {

    private IMocksControl mockery;
    private SfnClient sfn;

    private TestWorkflow workflow;
    private AnotherWorkflow anotherWorkflow;
    private List<Workflow> allWorkflows;
    private FluxCapacitorImpl fc;
    private FluxCapacitorConfig config;

    @BeforeEach
    public void setup() {
        workflow = new TestWorkflow();
        anotherWorkflow = new AnotherWorkflow();
        allWorkflows = List.of(workflow, anotherWorkflow);

        mockery = EasyMock.createControl();
        sfn = mockery.createMock(SfnClient.class);

        config  = new FluxCapacitorConfig();
        config.setAwsAccountId("123456789012");

        fc = new FluxCapacitorImpl(new NoopMetricRecorderFactory(), sfn, config, Clock.systemUTC());
    }

    @Test
    public void getRemoteWorkflowExecutor() {
        RemoteSfnClientConfig rcc = new RemoteSfnClientConfig();
        rcc.setAwsRegion("us-east-2");

        config.setRemoteSfnClientConfigProvider((endpointId) -> rcc);

        RemoteWorkflowExecutor rwe = fc.getRemoteWorkflowExecutor("some-endpoint");
        Assertions.assertNotNull(rwe);
        Assertions.assertEquals(RemoteWorkflowExecutorImpl.class, rwe.getClass());
    }

    @Test
    public void getRemoteWorkflowExecutorThrowsIfNoProviderConfigured() {
        config.setRemoteSfnClientConfigProvider(null);

        Assertions.assertThrows(IllegalStateException.class, () -> fc.getRemoteWorkflowExecutor("some-endpoint"));
    }

    @Test
    public void getRemoteWorkflowExecutorThrowsIfProviderProvidesNullConfig() {
        config.setRemoteSfnClientConfigProvider((endpointId) -> {
            if (endpointId.equals("some-endpoint")) {
                RemoteSfnClientConfig rcc = new RemoteSfnClientConfig();
                rcc.setAwsRegion("us-east-2");
                return rcc;
            }
            return null;
        });

        RemoteWorkflowExecutor rwe = fc.getRemoteWorkflowExecutor("some-endpoint");
        Assertions.assertNotNull(rwe);
        Assertions.assertEquals(RemoteWorkflowExecutorImpl.class, rwe.getClass());

        Assertions.assertThrows(IllegalStateException.class, () -> fc.getRemoteWorkflowExecutor("fake-endpoint"));
    }

    @Test
    public void testBuildSfnActivityName() {
        Assertions.assertEquals("TestWorkflow-StepOne", fc.buildSfnActivityName(TestWorkflow.class, StepOne.class));
    }

    @Test
    public void populateMapsPopulatesMaps() {
        fc.populateMaps(allWorkflows);
        Assertions.assertEquals(2, fc.getWorkflowsByClass().size());
        Assertions.assertEquals(workflow, fc.getWorkflowsByClass().get(TestWorkflow.class));
        Assertions.assertEquals(anotherWorkflow, fc.getWorkflowsByClass().get(AnotherWorkflow.class));

        Assertions.assertEquals(5, fc.getActivitiesByName().size());
        Assertions.assertEquals(workflow.stepOne, fc.getActivitiesByName().get(fc.buildSfnActivityName(TestWorkflow.class, StepOne.class)));
        Assertions.assertEquals(workflow.stepTwo, fc.getActivitiesByName().get(fc.buildSfnActivityName(TestWorkflow.class, StepTwo.class)));
        Assertions.assertEquals(anotherWorkflow.stepOne, fc.getActivitiesByName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepOne.class)));
        Assertions.assertEquals(anotherWorkflow.stepTwo, fc.getActivitiesByName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepTwo.class)));
        Assertions.assertEquals(anotherWorkflow.stepThree, fc.getActivitiesByName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepThree.class)));

        Assertions.assertEquals(5, fc.getWorkflowsByActivityName().size());
        Assertions.assertEquals(workflow, fc.getWorkflowsByActivityName().get(fc.buildSfnActivityName(TestWorkflow.class, StepOne.class)));
        Assertions.assertEquals(workflow, fc.getWorkflowsByActivityName().get(fc.buildSfnActivityName(TestWorkflow.class, StepTwo.class)));
        Assertions.assertEquals(anotherWorkflow, fc.getWorkflowsByActivityName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepOne.class)));
        Assertions.assertEquals(anotherWorkflow, fc.getWorkflowsByActivityName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepTwo.class)));
        Assertions.assertEquals(anotherWorkflow, fc.getWorkflowsByActivityName().get(fc.buildSfnActivityName(AnotherWorkflow.class, StepThree.class)));
    }

    // Minor naming gymnastics so we can put two workflow steps with the same class name into a workflow graph,
    // or define two different workflow classes with the same class name. This isn't actually allowed, so
    // we're doing it to ensure the relevant test fails.
    static class ContainerClass {
        static class TestWorkflow implements Workflow {
            private final WorkflowGraph graph;

            TestWorkflow() {
                StepOne stepOne = new StepOne();
                WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne);
                builder.alwaysClose(stepOne);
                this.graph = builder.build();
            }

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        }

        static class StepOne implements WorkflowStep {
            @StepApply
            public void doThing() {}
        }
    }

    @Test
    public void populateMapsDetectsWorkflowNameCollision() {
        Workflow wf1 = new TestWorkflow();
        Workflow wf2 = new ContainerClass.TestWorkflow();

        // make sure the test fails if someone accidentally renames one of those workflows
        Assertions.assertEquals(wf1.getClass().getSimpleName(), wf2.getClass().getSimpleName());

        try {
            fc.populateMaps(List.of(wf1, wf2));
            Assertions.fail("Should not be allowed to register two workflows with the same class name");
        } catch (FluxException e) {
            Assertions.assertTrue(e.getMessage().contains(TaskNaming.workflowName(wf1)));
        }
    }

    static class WorkflowWithActivityNameCollision implements Workflow {
        private final WorkflowGraph graph;

        WorkflowWithActivityNameCollision() {
            StepOne outerStepOne = new StepOne();
            ContainerClass.StepOne innerStepOne = new ContainerClass.StepOne();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(outerStepOne);
            builder.alwaysTransition(outerStepOne, innerStepOne);

            builder.addStep(innerStepOne);
            builder.alwaysClose(innerStepOne);

            this.graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    @Test
    public void populateMapsDetectsActivityNameCollision() {
        Workflow wf = new WorkflowWithActivityNameCollision();

        try {
            fc.populateMaps(Collections.singletonList(wf));
            Assertions.fail("Should not be allowed to register a workflow with two steps having the same class name");
        } catch (FluxException e) {
            Assertions.assertTrue(e.getMessage().contains(StepOne.class.getSimpleName()));
        }
    }

    @Test
    public void registersActivitiesIfNotAlreadyRegistered() {
        fc.populateMaps(allWorkflows);

        expectDescribeActivities(false);

        for (Workflow w : allWorkflows) {
            for (WorkflowGraphNode node : w.getGraph().getNodes().values()) {
                String activityName = fc.buildSfnActivityName(w.getClass(), node.getStep().getClass());
                CreateActivityRequest register = FluxCapacitorImpl.buildCreateActivityRequest(activityName);

                EasyMock.expect(sfn.createActivity(register)).andReturn(CreateActivityResponse.builder().build());
            }
        }

        mockery.replay();
        fc.registerActivities();
        mockery.verify();
    }

    @Test
    public void registersActivitiesIfNotAlreadyRegistered_HandlesThrottling() {
        fc.populateMaps(allWorkflows);

        expectDescribeActivities(false);

        for (Workflow w : allWorkflows) {
            for (WorkflowGraphNode node : w.getGraph().getNodes().values()) {
                String activityName = fc.buildSfnActivityName(w.getClass(), node.getStep().getClass());
                CreateActivityRequest register = FluxCapacitorImpl.buildCreateActivityRequest(activityName);

                EasyMock.expect(sfn.createActivity(register)).andThrow(SdkServiceException.builder().statusCode(429).build());
                EasyMock.expect(sfn.createActivity(register)).andReturn(CreateActivityResponse.builder().build());
            }
        }

        mockery.replay();
        fc.registerActivities();
        mockery.verify();
    }

    @Test
    public void doesNotRegisterActivitiesIfAlreadyRegistered() {
        fc.populateMaps(allWorkflows);

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
                stringBuffer.append("request{");
                stringBuffer.append(request);
                stringBuffer.append("}");
            }
        });
        return request;
    }

    private void expectDescribeActivities(boolean shouldExist) {
        ListActivitiesResponse response;
        if(shouldExist) {
            List<ActivityListItem> activities = new ArrayList<>();
            for (Workflow w : allWorkflows) {
                for (WorkflowGraphNode node : w.getGraph().getNodes().values()) {
                    String activityName = fc.buildSfnActivityName(w.getClass(), node.getStep().getClass());
                    ActivityListItem item = ActivityListItem.builder().name(activityName).build();
                    activities.add(item);
                }
            }
            response = ListActivitiesResponse.builder().activities(activities).build();
        } else {
            response = ListActivitiesResponse.builder().activities(Collections.emptyList()).build();
        }

        ListActivitiesRequest request = ListActivitiesRequest.builder().build();
        EasyMock.expect(sfn.listActivities(sdkFieldMatcher(request))).andReturn(response); // called internally by the iterable
        EasyMock.expect(sfn.listActivitiesPaginator(request)).andReturn(new ListActivitiesIterable(sfn, request));
    }

    public static class TestWorkflow implements Workflow {
        final WorkflowStep stepOne;
        final WorkflowStep stepTwo;

        private final WorkflowGraph graph;

        TestWorkflow() {
            stepOne = new StepOne();
            stepTwo = new StepTwo();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne);
            builder.alwaysTransition(stepOne, stepTwo);

            builder.addStep(stepTwo);
            builder.alwaysClose(stepTwo);

            this.graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    public static class AnotherWorkflow implements Workflow {
        final WorkflowStep stepOne;
        final WorkflowStep stepTwo;
        final WorkflowStep stepThree;

        private final WorkflowGraph graph;

        AnotherWorkflow() {
            stepOne = new StepOne();
            stepTwo = new StepTwo();
            stepThree = new StepThree();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne);
            builder.alwaysTransition(stepOne, stepTwo);

            builder.addStep(stepTwo);
            builder.alwaysTransition(stepTwo, stepThree);

            builder.addStep(stepThree);
            builder.alwaysClose(stepThree);

            this.graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    public static class StepOne implements WorkflowStep {
        @StepApply
        public void doThing() {}
    }

    public static class StepTwo implements WorkflowStep {
        @StepApply
        public void doThing() {}
    }

    public static class StepThree implements WorkflowStep {
        @StepApply
        public void doThing() {}
    }
}
