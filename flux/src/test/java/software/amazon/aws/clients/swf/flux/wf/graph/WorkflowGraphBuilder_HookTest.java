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

package software.amazon.aws.clients.swf.flux.wf.graph;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPartitionedStep;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPostStepHook;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPreAndPostStepHook;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestPreStepHook;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepDeclaresOutputAttribute;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepHasInputAttribute;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepOne;
import software.amazon.aws.clients.swf.flux.poller.testwf.TestStepTwo;
import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepHook;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepHook;

public class WorkflowGraphBuilder_HookTest {

    @Test
    public void addHook_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStepHook(one, hook);
        builder.alwaysClose(one);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
    }

    @Test
    public void addMultipleHooks_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStepHook(one, hook);
        builder.addStepHook(one, hook2);
        builder.alwaysClose(one);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertEquals(hook2, graph.getHooksForStep(one.getClass()).get(1));
    }

    @Test
    public void addSameHookMultipleTimes_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStepHook(one, hook);
        builder.addStepHook(one, hook);
        builder.alwaysClose(one);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(1));
    }

    @Test
    public void addHookWithPreAndPostMethods_Succeeds() {
        TestPreAndPostStepHook hook = new TestPreAndPostStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStepHook(one, hook);
        builder.alwaysClose(one);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
    }

    @Test
    public void addHook_ThrowsIfHookHasMultiplePreHookMethods() {
        WorkflowStepHook doNotDoThis = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook1() {}

            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook2() {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStepHook(one, doNotDoThis);
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addHook_ThrowsIfHookHasMultiplePostHookMethods() {
        WorkflowStepHook doNotDoThis = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook1() {}

            @StepHook(hookType = StepHook.HookType.POST)
            public void hook2() {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStepHook(one, doNotDoThis);
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addHook_ThrowsIfStepBeingHookedDoesNotExist() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        WorkflowStep two = new TestStepTwo();

        try {
            builder.addStepHook(two, new TestPreStepHook());
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addHookToAllSteps_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hook);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
    }

    @Test
    public void addHookToAllSteps_WithMultipleSteps_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hook);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertNotNull(graph.getHooksForStep(two.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(two.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(two.getClass()).get(0));
    }

    @Test
    public void addMultipleHooksToAllSteps_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hook);
        builder.addHookForAllSteps(hook2);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertEquals(hook2, graph.getHooksForStep(one.getClass()).get(1));
    }

    @Test
    public void addMultipleHooksToAllSteps_WithMultipleSteps_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hook);
        builder.addHookForAllSteps(hook2);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertEquals(hook2, graph.getHooksForStep(one.getClass()).get(1));
        Assert.assertNotNull(graph.getHooksForStep(two.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(two.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(two.getClass()).get(0));
        Assert.assertEquals(hook2, graph.getHooksForStep(two.getClass()).get(1));
    }

    @Test
    public void addSameHookMultipleTimesToAllSteps_Succeeds() {
        TestPreStepHook hook = new TestPreStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hook);
        builder.addHookForAllSteps(hook);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(2, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(1));
    }

    @Test
    public void addHookWithPreAndPostMethodsToAllSteps_Succeeds() {
        TestPreAndPostStepHook hook = new TestPreAndPostStepHook();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hook);

        WorkflowGraph graph = builder.build();
        Assert.assertNotNull(graph);
        Assert.assertNotNull(graph.getHooksForStep(one.getClass()));
        Assert.assertEquals(1, graph.getHooksForStep(one.getClass()).size());
        Assert.assertEquals(hook, graph.getHooksForStep(one.getClass()).get(0));
    }

    @Test
    public void addHookToAllSteps_ThrowsIfHookHasMultiplePreHookMethods() {
        WorkflowStepHook doNotDoThis = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook1() {}

            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook2() {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addHookForAllSteps(doNotDoThis);
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addHookToAllSteps_ThrowsIfHookHasMultiplePostHookMethods() {
        WorkflowStepHook doNotDoThis = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook1() {}

            @StepHook(hookType = StepHook.HookType.POST)
            public void hook2() {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addHookForAllSteps(doNotDoThis);
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void hook_DoesNotValidateAttributeInputsIfInitialInputAttributesNotSpecified() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute("SomeAttribute") String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowMetrics() {
        WorkflowStepHook hookWithMetricsAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(MetricRecorder metrics) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(MetricRecorder metrics) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithMetricsAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailableButOptional() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableInInitialInput() {
        final String attributeName = "SomeAttribute";
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(attributeName) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(attributeName) String foo) {}
        };

        Map<String, Class<?>> initialAttributes = new HashMap<>();
        initialAttributes.put(attributeName, String.class);

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, initialAttributes);
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromPreviousStep() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addStepHook(two, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowStepSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityInitialAttemptTime,
                                @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt,
                                @Attribute(StepAttributes.WORKFLOW_START_TIME) Date workflowStartTime) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityInitialAttemptTime,
                                 @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt,
                                 @Attribute(StepAttributes.WORKFLOW_START_TIME) Date workflowStartTime) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId, @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId, @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowResultCode() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowActivityCompletionMessage() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowPostHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode, @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowPartitionAttributesWhenHookingPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.PARTITION_ID) String partitionId, @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.PARTITION_ID) String partitionId, @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCountWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCountWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void hookAllSteps_DoesNotValidateAttributeInputsIfInitialInputAttributesNotSpecified() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute("SomeAttribute") String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addStepHook(one, hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowMetrics() {
        WorkflowStepHook hookWithMetricsAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(MetricRecorder metrics) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(MetricRecorder metrics) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithMetricsAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailableButOptional() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableInInitialInput() {
        final String attributeName = "SomeAttribute";
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(attributeName) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(attributeName) String foo) {}
        };

        Map<String, Class<?>> initialAttributes = new HashMap<>();
        initialAttributes.put(attributeName, String.class);

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, initialAttributes);
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void prehookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromFirstStep_Disallow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void prehookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_OptionalAttributeAvailableFromFirstStep_Allow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(value=TestStepHasInputAttribute.INPUT_ATTR, optional=true) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromFirstStep_Allow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowStepSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityInitialAttemptTime,
                             @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt,
                             @Attribute(StepAttributes.WORKFLOW_START_TIME) Date workflowStartTime) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityInitialAttemptTime,
                             @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt,
                             @Attribute(StepAttributes.WORKFLOW_START_TIME) Date workflowStartTime) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId,
                                @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId,
                                 @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowResultCode() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowActivityCompletionMessage() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowPostHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode,
                             @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void hookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowPartitionAttributesWhenHookingPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                                @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                                 @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCountWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCountWhenHookingNonPartitionedStep() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingSomeNonPartitionedSteps() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestPartitionedStep();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postHookAllSteps_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionIdWhenHookingSomeNonPartitionedSteps() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestPartitionedStep();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addHookForAllSteps(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void workflowHook_PreHookAddsPreWorkflowHookAnchor() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);
        builder.addWorkflowHook(new TestPreStepHook());

        WorkflowGraph graph = builder.build();
        Assert.assertEquals(PreWorkflowHookAnchor.class, graph.getFirstStep().getClass());
        Assert.assertNull(graph.getHooksForStep(TestStepOne.class));
        Assert.assertNotNull(graph.getHooksForStep(PreWorkflowHookAnchor.class));
        Assert.assertEquals(1, graph.getHooksForStep(PreWorkflowHookAnchor.class).size());
        Assert.assertEquals(TestPreStepHook.class, graph.getHooksForStep(PreWorkflowHookAnchor.class).get(0).getClass());

        Assert.assertFalse(graph.getNodes().containsKey(PostWorkflowHookAnchor.class));
    }

    @Test
    public void workflowHook_PostHookAddsPostWorkflowHookAnchor() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);
        builder.addWorkflowHook(new TestPostStepHook());

        WorkflowGraph graph = builder.build();
        Assert.assertEquals(PostWorkflowHookAnchor.class, graph.getNodes().get(TestStepOne.class).getNextStepsByResultCode().get(StepResult.ALWAYS_RESULT_CODE).getStep().getClass());
        Assert.assertEquals(TestStepOne.class, graph.getFirstStep().getClass());
        Assert.assertNull(graph.getHooksForStep(TestStepOne.class));
        Assert.assertNotNull(graph.getHooksForStep(PostWorkflowHookAnchor.class));
        Assert.assertEquals(1, graph.getHooksForStep(PostWorkflowHookAnchor.class).size());
        Assert.assertEquals(TestPostStepHook.class, graph.getHooksForStep(PostWorkflowHookAnchor.class).get(0).getClass());

        Assert.assertEquals(1, graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().size());
        Assert.assertTrue(graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().containsKey(StepResult.ALWAYS_RESULT_CODE));
        Assert.assertNull(graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().get(StepResult.ALWAYS_RESULT_CODE));

        Assert.assertFalse(graph.getNodes().containsKey(PreWorkflowHookAnchor.class));
    }

    @Test
    public void workflowHook_PreAndPostHookAddsPreAndPostWorkflowHookAnchors() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);
        builder.addWorkflowHook(new TestPreAndPostStepHook());

        WorkflowGraph graph = builder.build();
        Assert.assertEquals(PreWorkflowHookAnchor.class, graph.getFirstStep().getClass());
        Assert.assertEquals(PostWorkflowHookAnchor.class, graph.getNodes().get(TestStepOne.class).getNextStepsByResultCode().get(StepResult.ALWAYS_RESULT_CODE).getStep().getClass());

        Assert.assertNull(graph.getHooksForStep(TestStepOne.class));

        Assert.assertNotNull(graph.getHooksForStep(PreWorkflowHookAnchor.class));
        Assert.assertEquals(1, graph.getHooksForStep(PreWorkflowHookAnchor.class).size());
        Assert.assertEquals(TestPreAndPostStepHook.class, graph.getHooksForStep(PreWorkflowHookAnchor.class).get(0).getClass());

        Assert.assertNotNull(graph.getHooksForStep(PostWorkflowHookAnchor.class));
        Assert.assertEquals(1, graph.getHooksForStep(PostWorkflowHookAnchor.class).size());
        Assert.assertEquals(TestPreAndPostStepHook.class, graph.getHooksForStep(PostWorkflowHookAnchor.class).get(0).getClass());

        Assert.assertEquals(1, graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().size());
        Assert.assertTrue(graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().containsKey(StepResult.ALWAYS_RESULT_CODE));
        Assert.assertNull(graph.getNodes().get(PostWorkflowHookAnchor.class).getNextStepsByResultCode().get(StepResult.ALWAYS_RESULT_CODE));
    }

    @Test
    public void workflowHook_DoesNotValidateAttributeInputsIfInitialInputAttributesNotSpecified() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute("SomeAttribute") String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute("SomeAttribute") String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void workflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowMetrics() {
        WorkflowStepHook hookWithMetricsAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(MetricRecorder metrics) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(MetricRecorder metrics) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithMetricsAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void workflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailableButOptional() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(value="SomeAttribute", optional=true) String foo) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void workflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableInInitialInput() {
        final String attributeName = "SomeAttribute";
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(attributeName) String foo) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(attributeName) String foo) {}
        };

        Map<String, Class<?>> initialAttributes = new HashMap<>();
        initialAttributes.put(attributeName, String.class);

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, initialAttributes);
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromAStep_Disallow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_OptionalAttributeAvailableFromAStep_Allow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(value=TestStepHasInputAttribute.INPUT_ATTR, optional=true) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void postWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromAStep_Allow() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(TestStepHasInputAttribute.INPUT_ATTR) String foo) {}
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void workflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void prehook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId, @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
            @StepHook(hookType = StepHook.HookType.POST)
            public void posthook(@Attribute(StepAttributes.WORKFLOW_ID) String wfId, @Attribute(StepAttributes.ACTIVITY_NAME) String stepName) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowResultCode() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode, @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowActivityCompletionMessage() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowPostHookSpecificAttributes() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.RESULT_CODE) String resultCode, @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String message) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        Assert.assertNotNull(builder.build());
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionId() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void preWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCount() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.PRE)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionId() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void postWorkflowHook_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowPartitionCount() {
        WorkflowStepHook hookWithInputAttribute = new WorkflowStepHook() {
            @StepHook(hookType = StepHook.HookType.POST)
            public void hook(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {}
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        builder.addWorkflowHook(hookWithInputAttribute);

        try {
            builder.build();
            Assert.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }
}
