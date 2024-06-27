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

package com.danielgmyers.flux.step.internal;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepHook;
import com.danielgmyers.flux.step.StepInputAccessor;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepHook;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.PostWorkflowHookAnchor;
import com.danielgmyers.flux.wf.graph.PreWorkflowHookAnchor;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ActivityExecutionUtilTest {

    private DummyStep step;
    private StepWithSeveralInputs stepWithSeveralInputs;
    private InMemoryMetricRecorder fluxMetrics;
    private InMemoryMetricRecorder stepMetrics;

    private StepInputAccessor emptyInput;

    @BeforeEach
    public void setup() {
        step = new DummyStep();
        stepWithSeveralInputs = new StepWithSeveralInputs();

        fluxMetrics = new InMemoryMetricRecorder("ActivityExecutionUtil");
        stepMetrics = new InMemoryMetricRecorder(TaskNaming.activityName(TestWorkflow.class, DummyStep.class));

        emptyInput = new StepInputAccessor() {
            @Override
            public <T> T getAttribute(Class<T> requestedType, String attributeName) {
                return null;
            }
        };
    }

    @Test
    public void testExecuteActivity_returnsCompleteForSuccessCase() {
        WorkflowGraphBuilder graph = new WorkflowGraphBuilder(step);
        graph.alwaysClose(step);

        Workflow workflow = new TestWorkflow(graph.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);

        String activityName = TaskNaming.activityName(workflow, step);
        StepResult actualResult = ActivityExecutionUtil.executeActivity(step, activityName, fluxMetrics, stepMetrics, emptyInput);
        Assertions.assertTrue(step.didThing());

        // fluxMetrics needs to be closed by the caller to executeActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                result.getResultCode())).intValue());
    }

    @Test
    public void testExecuteActivity_returnsRetryWhenApplyReturnsRetry() {
        WorkflowGraphBuilder graph = new WorkflowGraphBuilder(step);
        graph.alwaysClose(step);

        Workflow workflow = new TestWorkflow(graph.build());

        StepResult expectedResult = makeStepResult(StepResult.ResultAction.RETRY, null, "hmm", Collections.emptyMap());
        step.setStepResult(expectedResult);

        String activityName = TaskNaming.activityName(workflow, step);
        StepResult actualResult = ActivityExecutionUtil.executeActivity(step, activityName, fluxMetrics, stepMetrics, emptyInput);
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry

        // fluxMetrics needs to be closed by the caller to executeActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(TaskNaming.activityName(workflow, step),
                null)).intValue());
    }

    @Test
    public void testExecuteActivity_returnsRetryWhenApplyThrowsException() {
        WorkflowGraphBuilder graph = new WorkflowGraphBuilder(step);
        graph.alwaysClose(step);

        Workflow workflow = new TestWorkflow(graph.build());

        RuntimeException e = new RuntimeException("message!");
        step.setExceptionToThrow(e);

        StepResult expectedResult = StepResult.retry(e);

        String activityName = TaskNaming.activityName(workflow, step);
        StepResult actualResult = ActivityExecutionUtil.executeActivity(step, activityName, fluxMetrics, stepMetrics, emptyInput);
        Assertions.assertFalse(step.didThing()); // false because the step threw an exception in the middle of doing the thing

        // fluxMetrics needs to be closed by the caller to executeActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(TaskNaming.activityName(workflow, step),
                e.getClass().getSimpleName())).intValue());
    }

    @Test
    public void testExecuteActivity_properlyMapsAttributesToParameters() {
        WorkflowGraphBuilder graph = new WorkflowGraphBuilder(stepWithSeveralInputs);
        graph.alwaysClose(stepWithSeveralInputs);

        Workflow workflow = new TestWorkflow(graph.build());

        String stringParam = "foo";
        Long longParam = 42L;
        Boolean booleanParam = true;
        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("bar", "baz");
        stringMap.put("zap", null);

        stepWithSeveralInputs = new StepWithSeveralInputs();
        stepWithSeveralInputs.setExpectedString(stringParam);
        stepWithSeveralInputs.setExpectedLong(longParam);
        stepWithSeveralInputs.setExpectedBoolean(booleanParam);
        stepWithSeveralInputs.setExpectedStringMap(stringMap);

        Map<String, Object> input = new HashMap<>();
        input.put(StepWithSeveralInputs.STRING_PARAM, stringParam);
        input.put(StepWithSeveralInputs.LONG_PARAM, longParam);
        input.put(StepWithSeveralInputs.BOOLEAN_PARAM, booleanParam);
        input.put(StepWithSeveralInputs.STRING_MAP_PARAM, stringMap);

        StepInputAccessor inputAccessor = new StepInputAccessor() {
            @Override
            public <T> T getAttribute(Class<T> requestedType, String attributeName) {
                return (T)(input.get(attributeName));
            }
        };

        String activityName = TaskNaming.activityName(workflow, stepWithSeveralInputs);
        StepResult actualResult = ActivityExecutionUtil.executeActivity(stepWithSeveralInputs, activityName, fluxMetrics, stepMetrics, inputAccessor);

        // The stub step has a return type of void, so it should always just be a plain success result.
        Assertions.assertEquals(StepResult.success(), actualResult);

        // fluxMetrics needs to be closed by the caller to executeActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(activityName,
                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    public static class StepWithMetrics implements WorkflowStep {

        private final String metricName;

        public StepWithMetrics(String metricName) {
            this.metricName = metricName;
        }
        @StepApply
        public void doThing(MetricRecorder metrics) {
            metrics.addCount(metricName, 1.0);
        }
    }

    @Test
    public void testExecuteActivity_passesInStepMetrics() {
        final String stepMetric = "foo";

        WorkflowStep stepWithMetrics = new StepWithMetrics(stepMetric);
        WorkflowGraphBuilder graph = new WorkflowGraphBuilder(stepWithMetrics);
        graph.alwaysClose(stepWithMetrics);

        Workflow workflow = new TestWorkflow(graph.build());

        String activityName = TaskNaming.activityName(workflow, stepWithMetrics);
        StepResult actualResult = ActivityExecutionUtil.executeActivity(stepWithMetrics, activityName, fluxMetrics, stepMetrics, emptyInput);

        // The stub step has a return type of void, so it should always just be a plain success result.
        Assertions.assertEquals(StepResult.success(), actualResult);

        // the metrics objects need to be closed by the caller to executeActivity.
        // We need to close them so we can query their data.
        fluxMetrics.close();
        stepMetrics.close();

        Assertions.assertTrue(stepMetrics.getCounts().containsKey(stepMetric));
        Assertions.assertEquals(1, stepMetrics.getCounts().get(stepMetric).longValue());

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(activityName,
                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_runsPreAndPostHooksForStep() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);
        builder.addStepHook(step, hook2);
        builder.addStepHook(step, hook3);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        Map<String, Object> fullOutput = new HashMap<>(output);
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", fullOutput);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertTrue(step.didThing());
        Assertions.assertEquals(1, hook.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPostStepHookCallCount());

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreStepHook.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, step))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostStepHook.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, step))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, step))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, step))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                result.getResultCode())).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_runsPreAndPostHooksForStep_PassInHookMetrics() {
        WorkflowStepHook metricsHook = new TestHookWithMetrics();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, metricsHook);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(expectedResult);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertTrue(step.didThing());

        // both metrics objects needs to be closed by the caller to executeHooksAndActivity.
        // We need to close them so we can query their data.
        fluxMetrics.close();
        stepMetrics.close();

        Assertions.assertTrue(stepMetrics.getCounts().containsKey(TestHookWithMetrics.PRE_HOOK_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.getCounts().containsKey(TestHookWithMetrics.POST_HOOK_METRIC_NAME));
        Assertions.assertEquals(1, stepMetrics.getCounts().get(TestHookWithMetrics.PRE_HOOK_METRIC_NAME).longValue());
        Assertions.assertEquals(1, stepMetrics.getCounts().get(TestHookWithMetrics.POST_HOOK_METRIC_NAME).longValue());

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestHookWithMetrics.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, step))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestHookWithMetrics.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, step))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                expectedResult.getResultCode())).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_runsOnlyPreHooksForPreWorkflowHookAnchor() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addWorkflowHook(hook);
        builder.addWorkflowHook(hook2);
        builder.addWorkflowHook(hook3);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = new HashMap<>();
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, null, output);

        WorkflowStep anchorStep = workflow.getGraph().getFirstStep();
        Assertions.assertEquals(PreWorkflowHookAnchor.class, anchorStep.getClass());

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, anchorStep, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertEquals(1, hook.getPreStepHookCallCount());
        Assertions.assertEquals(0, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(0, hook3.getPostStepHookCallCount());

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreStepHook.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, anchorStep))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, anchorStep))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, anchorStep),
                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_runsOnlyPostHooksForPostWorkflowHookAnchor() {
        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addWorkflowHook(hook);
        builder.addWorkflowHook(hook2);
        builder.addWorkflowHook(hook3);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = new HashMap<>();
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, null, output);

        Assertions.assertNotNull(workflow.getGraph().getNodes().get(PostWorkflowHookAnchor.class));
        WorkflowStep anchorStep = workflow.getGraph().getNodes().get(PostWorkflowHookAnchor.class).getStep();
        Assertions.assertNotNull(anchorStep);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, anchorStep, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertEquals(0, hook.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(0, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPostStepHookCallCount());

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostStepHook.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, anchorStep))));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, anchorStep))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, anchorStep),
                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_ignoresFailureInPreHook_HookRetryOnFailureDisabled() {
        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPreHookThrowsExceptionNoRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(expectedResult);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertTrue(step.didThing());

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreHookThrowsExceptionNoRetryOnFailure.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, step))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                expectedResult.getResultCode())).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_retryOnFailureInPreHook_HookRetryOnFailureEnabled() {
        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPreHookThrowsExceptionRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new TestWorkflow(builder.build());

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertFalse(step.didThing()); // false because the pre-hook required a retry on failure, so the step never ran

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(StepResult.ResultAction.RETRY, actualResult.getAction());
        Assertions.assertNull(actualResult.getResultCode());
        Assertions.assertNotNull(actualResult.getMessage());
        Assertions.assertTrue(actualResult.getMessage().contains(TestPreHookThrowsExceptionRetryOnFailure.class.getSimpleName())
                              && actualResult.getMessage().contains(e.getMessage()));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreHookThrowsExceptionRetryOnFailure.class.getSimpleName(),
                "preStepHook", TaskNaming.activityName(workflow, step))));
    }

    @Test
    public void testExecuteHooksAndActivity_ignoresFailureInPostHook_HookRetryOnFailureDisabled() {
        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPostHookThrowsExceptionNoRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult expectedResult = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(expectedResult);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertTrue(step.didThing());

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(expectedResult, actualResult);

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostHookThrowsExceptionNoRetryOnFailure.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, step))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                expectedResult.getResultCode())).intValue());
    }

    @Test
    public void testExecuteHooksAndActivity_retryOnFailureInPostHook_HookRetryOnFailureEnabled() {
        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPostHookThrowsExceptionRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new TestWorkflow(builder.build());

        Map<String, Object> output = Collections.emptyMap();
        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        StepResult actualResult = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, emptyInput, fluxMetrics, stepMetrics);
        Assertions.assertTrue(step.didThing()); // true because the step runs before the hook

        // fluxMetrics needs to be closed by the caller to executeHooksAndActivity.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(StepResult.ResultAction.RETRY, actualResult.getAction());
        Assertions.assertNull(actualResult.getResultCode());
        Assertions.assertNotNull(actualResult.getMessage());
        Assertions.assertTrue(actualResult.getMessage().contains(TestPostHookThrowsExceptionRetryOnFailure.class.getSimpleName())
                && actualResult.getMessage().contains(e.getMessage()));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostHookThrowsExceptionRetryOnFailure.class.getSimpleName(),
                "postStepHook", TaskNaming.activityName(workflow, step))));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(TaskNaming.activityName(workflow, step),
                result.getResultCode())).intValue());
    }

    private StepResult makeStepResult(StepResult.ResultAction resultAction, String stepResult, String message, Map<String, Object> output) {
        return new StepResult(resultAction, stepResult, message).withAttributes(output);
    }

    public static class TestWorkflow implements Workflow {

        private final WorkflowGraph graph;

        public TestWorkflow(WorkflowGraph graph) {
            this.graph = graph;
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    public static class DummyStep implements WorkflowStep {

        private boolean didThing = false;
        private RuntimeException exceptionToThrow = null;
        private StepResult result = null;
        private long sleepDurationMillis = 0;

        public void setExceptionToThrow(RuntimeException e) {
            this.exceptionToThrow = e;
        }

        public void setStepResult(StepResult result) {
            this.result = result;
        }

        public void setSleepDurationMillis(long sleepDurationMillis) {
            this.sleepDurationMillis = sleepDurationMillis;
        }

        @StepApply
        public StepResult doThing() throws InterruptedException {
            if(exceptionToThrow != null) {
                throw exceptionToThrow;
            }

            if(sleepDurationMillis > 0) {
                Thread.sleep(sleepDurationMillis);
            }

            didThing = true;
            return result;
        }

        public boolean didThing() {
            return didThing;
        }
    }

    public static class StepWithSeveralInputs implements WorkflowStep {

        public static final String STRING_PARAM = "someStringValue";
        public static final String LONG_PARAM = "someLongValue";
        public static final String BOOLEAN_PARAM = "someBooleanValue";
        public static final String STRING_MAP_PARAM = "someStringMap";

        private String expectedString;
        private Long expectedLong;
        private Boolean expectedBoolean;
        private Map<String, String> expectedStringMap;

        @StepApply
        public void apply(@Attribute(STRING_PARAM) String stringParam,
                          @Attribute(LONG_PARAM) Long longParam,
                          @Attribute(BOOLEAN_PARAM) Boolean booleanParam,
                          @Attribute(STRING_MAP_PARAM) Map<String, String> stringMap,
                          @Attribute("ThisShouldBeNull") String nullParam) {
            Assertions.assertNotNull(stringParam);
            Assertions.assertNotNull(longParam);
            Assertions.assertNotNull(booleanParam);
            Assertions.assertNotNull(stringMap);
            Assertions.assertNull(nullParam);

            Assertions.assertEquals(expectedString, stringParam);
            Assertions.assertEquals(expectedLong, longParam);
            Assertions.assertEquals(expectedBoolean, booleanParam);
            Assertions.assertEquals(expectedStringMap, stringMap);
        }

        public void setExpectedString(String expectedString) {
            this.expectedString = expectedString;
        }

        public void setExpectedLong(Long expectedLong) {
            this.expectedLong = expectedLong;
        }

        public void setExpectedBoolean(Boolean expectedBoolean) {
            this.expectedBoolean = expectedBoolean;
        }

        public void setExpectedStringMap(Map<String, String> expectedStringMap) {
            this.expectedStringMap = expectedStringMap;
        }
    }

    public static class TestPreStepHook implements WorkflowStepHook {

        private int preStepHookCallCount = 0;

        @StepHook(hookType = StepHook.HookType.PRE)
        public void preStepHook(@Attribute(StepAttributes.ACTIVITY_NAME) String activityName,
                                @Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date activityStartTime,
                                @Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                @Attribute(StepAttributes.WORKFLOW_START_TIME) String workflowStartTime) {
            preStepHookCallCount += 1;
        }

        public int getPreStepHookCallCount() {
            return preStepHookCallCount;
        }
    }

    public static class TestPostStepHook implements WorkflowStepHook {

        private int postStepHookCallCount = 0;

        @StepHook(hookType = StepHook.HookType.POST)
        public void postStepHook(@Attribute(StepAttributes.RESULT_CODE) String resultCode,
                                 @Attribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE) String completionMessage) {
            postStepHookCallCount += 1;
        }

        public int getPostStepHookCallCount() {
            return postStepHookCallCount;
        }
    }

    public static class TestPreAndPostStepHook implements WorkflowStepHook {

        private int preStepHookCallCount = 0;
        private int postStepHookCallCount = 0;

        @StepHook(hookType = StepHook.HookType.PRE)
        public void preStepHook() {
            preStepHookCallCount += 1;
        }

        @StepHook(hookType = StepHook.HookType.POST)
        public void postStepHook() {
            postStepHookCallCount += 1;
        }

        public int getPreStepHookCallCount() {
            return preStepHookCallCount;
        }

        public int getPostStepHookCallCount() {
            return postStepHookCallCount;
        }
    }

    public static class TestHookWithMetrics implements WorkflowStepHook {

        public static final String PRE_HOOK_METRIC_NAME = "preHook";
        public static final String POST_HOOK_METRIC_NAME = "postHook";

        @StepHook(hookType = StepHook.HookType.PRE)
        public void preStepHook(MetricRecorder metrics) {
            metrics.addCount(PRE_HOOK_METRIC_NAME, 1.0);
        }

        @StepHook(hookType = StepHook.HookType.POST)
        public void postStepHook(MetricRecorder metrics) {
            metrics.addCount(POST_HOOK_METRIC_NAME, 1.0);
        }

    }

    public static class TestPreHookThrowsExceptionNoRetryOnFailure implements WorkflowStepHook {
        private final Throwable ex;

        TestPreHookThrowsExceptionNoRetryOnFailure(Throwable ex) {
            this.ex = ex;
        }

        @StepHook(hookType = StepHook.HookType.PRE, retryOnFailure = false)
        public void preStepHook() throws Throwable {
            throw ex;
        }
    }

    public static class TestPreHookThrowsExceptionRetryOnFailure implements WorkflowStepHook {
        private final Throwable ex;

        TestPreHookThrowsExceptionRetryOnFailure(Throwable ex) {
            this.ex = ex;
        }

        @StepHook(hookType = StepHook.HookType.PRE, retryOnFailure = true)
        public void preStepHook() throws Throwable {
            throw ex;
        }
    }

    public static class TestPostHookThrowsExceptionNoRetryOnFailure implements WorkflowStepHook {
        private final Throwable ex;

        TestPostHookThrowsExceptionNoRetryOnFailure(Throwable ex) {
            this.ex = ex;
        }

        @StepHook(hookType = StepHook.HookType.POST, retryOnFailure = false)
        public void postStepHook() throws Throwable {
            throw ex;
        }
    }

    public static class TestPostHookThrowsExceptionRetryOnFailure implements WorkflowStepHook {
        private final Throwable ex;

        TestPostHookThrowsExceptionRetryOnFailure(Throwable ex) {
            this.ex = ex;
        }

        @StepHook(hookType = StepHook.HookType.POST, retryOnFailure = true)
        public void postStepHook() throws Throwable {
            throw ex;
        }
    }
}
