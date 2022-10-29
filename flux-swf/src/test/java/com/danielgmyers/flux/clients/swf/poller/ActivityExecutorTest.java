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

package com.danielgmyers.flux.clients.swf.poller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestHookWithMetrics;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPostStepHook;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPreAndPostStepHook;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestPreStepHook;
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.poller.ActivityExecutionUtil;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepHook;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepHook;
import com.danielgmyers.flux.step.WorkflowStepUtil;
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

import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;

public class ActivityExecutorTest {

    private static final String IDENTITY = "unit";
    private static final String STEP_NAME = "DummyWorkflow.DummyStep";
    private static final String TASK_TOKEN = "task-token";

    private ActivityTaskPollerTest.DummyStep step;
    private InMemoryMetricRecorder fluxMetrics;
    private InMemoryMetricRecorder stepMetrics;

    @BeforeEach
    public void setup() {
        step = new ActivityTaskPollerTest.DummyStep();
        fluxMetrics = new InMemoryMetricRecorder("ActivityExecutor");
        stepMetrics = new InMemoryMetricRecorder(STEP_NAME);
    }

    @Test
    public void appliesStepIfInputIsNull() {
        PollForActivityTaskResponse task = makeTask(null, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        Map<String, String> output = Collections.emptyMap();
        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                                                                                                      result.getResultCode())).intValue());
    }

    @Test
    public void appliesStepIfInputIsEmptyMap() {
        Map<String, String> input = Collections.emptyMap();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void returnsCompleteForSuccessCase() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void returnsRetryWhenApplyReturnsRetry() {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.RETRY, null, "hmm", Collections.emptyMap());
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertNull(executor.getOutput()); // null because retry doesn't support output attributes, and there was no exception stack trace to record

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(),
                                null)).intValue());
    }

    @Test
    public void returnsRetryWhenApplyThrowsException() {
        Map<String, String> input = new HashMap<>();

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        RuntimeException e = new RuntimeException("message!");
        step.setExceptionToThrow(e);

        executor.run();
        Assertions.assertFalse(step.didThing()); // false because the step threw an exception in the middle of doing the thing

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(stackTrace, executor.getOutput()); // the output should match the exception's stack trace

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.ResultAction.RETRY, executor.getResult().getAction());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(),
                                e.getClass().getSimpleName())).intValue());
    }

    @Test
    public void properlyMapsAttributesToParameters() {
        String stringParam = "foo";
        Long longParam = 42L;
        Boolean booleanParam = true;
        Map<String, String> stringMap = new HashMap<>();
        stringMap.put("bar", "baz");
        stringMap.put("zap", null);

        StubStep stub = new StubStep();
        stub.setExpectedString(stringParam);
        stub.setExpectedLong(longParam);
        stub.setExpectedBoolean(booleanParam);
        stub.setExpectedStringMap(stringMap);

        Map<String, String> input = new HashMap<>();
        input.put(StubStep.STRING_PARAM, StepAttributes.encode(stringParam));
        input.put(StubStep.LONG_PARAM, StepAttributes.encode(longParam));
        input.put(StubStep.BOOLEAN_PARAM, StepAttributes.encode(booleanParam));
        input.put(StubStep.STRING_MAP_PARAM, StepAttributes.encode(stringMap));

        String activityName = "stepName";

        // The stub step has a return type of void, so it should always just be a plain success result.
        Assertions.assertEquals(StepResult.success(), ActivityExecutionUtil.executeActivity(stub, activityName, fluxMetrics, stepMetrics, input));

        // fluxMetrics is usually closed by the ActivityTaskPoller.
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
    public void passesInStepMetrics() {
        final String stepMetric = "foo";

        WorkflowStep stepWithMetrics = new StepWithMetrics(stepMetric);
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepWithMetrics);
        builder.alwaysClose(stepWithMetrics);

        String activityName = "stepName";

        // The stub step has a return type of void, so it should always just be a plain success result.
        Assertions.assertEquals(StepResult.success(), ActivityExecutionUtil.executeActivity(stepWithMetrics, activityName, fluxMetrics, stepMetrics, Collections.emptyMap()));

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        // stepMetrics is usually closed by the ActivityTaskExecutor, outside the executeActivity call.
        // We need to close it so we can query its data.
        stepMetrics.close();

        Assertions.assertTrue(stepMetrics.getCounts().containsKey(stepMetric));
        Assertions.assertEquals(1, stepMetrics.getCounts().get(stepMetric).longValue());

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(activityName,
                                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    @Test
    public void runsPreAndPostHooksForStep() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);
        builder.addStepHook(step, hook2);
        builder.addStepHook(step, hook3);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());
        Assertions.assertEquals(1, hook.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPostStepHookCallCount());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreStepHook.class.getSimpleName(),
                "preStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostStepHook.class.getSimpleName(),
                "postStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "preStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "postStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void runsPreAndPostHooksForStep_PassInHookMetrics() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        WorkflowStepHook metricsHook = new TestHookWithMetrics();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, metricsHook);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertTrue(stepMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.getCounts().containsKey(TestHookWithMetrics.PRE_HOOK_METRIC_NAME));
        Assertions.assertTrue(stepMetrics.getCounts().containsKey(TestHookWithMetrics.POST_HOOK_METRIC_NAME));
        Assertions.assertEquals(1, stepMetrics.getCounts().get(TestHookWithMetrics.PRE_HOOK_METRIC_NAME).longValue());
        Assertions.assertEquals(1, stepMetrics.getCounts().get(TestHookWithMetrics.POST_HOOK_METRIC_NAME).longValue());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestHookWithMetrics.class.getSimpleName(),
                "preStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestHookWithMetrics.class.getSimpleName(),
                "postStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void runsOnlyPreHooksForPreWorkflowHookAnchor() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addWorkflowHook(hook);
        builder.addWorkflowHook(hook2);
        builder.addWorkflowHook(hook3);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        WorkflowStep anchorStep = workflow.getGraph().getFirstStep();
        Assertions.assertEquals(PreWorkflowHookAnchor.class, anchorStep.getClass());

        PollForActivityTaskResponse task = makeTask(input, TaskNaming.activityName(TaskNaming.workflowName(workflow.getClass()), anchorStep));
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, anchorStep, fluxMetrics,  (o, c) -> stepMetrics);

        executor.run();
        Assertions.assertEquals(1, hook.getPreStepHookCallCount());
        Assertions.assertEquals(0, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(0, hook3.getPostStepHookCallCount());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.RESULT_CODE, StepResult.SUCCEED_RESULT_CODE);

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.success(), executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreStepHook.class.getSimpleName(),
                "preStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "preStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                StepResult.SUCCEED_RESULT_CODE)).intValue());
    }

    @Test
    public void runsOnlyPostHooksForPostWorkflowHookAnchor() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        TestPreStepHook hook = new TestPreStepHook();
        TestPostStepHook hook2 = new TestPostStepHook();
        TestPreAndPostStepHook hook3 = new TestPreAndPostStepHook();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addWorkflowHook(hook);
        builder.addWorkflowHook(hook2);
        builder.addWorkflowHook(hook3);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        Assertions.assertNotNull(workflow.getGraph().getNodes().get(PostWorkflowHookAnchor.class));
        WorkflowStep anchorStep = workflow.getGraph().getNodes().get(PostWorkflowHookAnchor.class).getStep();
        Assertions.assertNotNull(anchorStep);

        PollForActivityTaskResponse task = makeTask(input, TaskNaming.activityName(TaskNaming.workflowName(workflow.getClass()), anchorStep));
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, anchorStep, fluxMetrics,  (o, c) -> stepMetrics);

        executor.run();
        Assertions.assertEquals(0, hook.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook2.getPostStepHookCallCount());
        Assertions.assertEquals(0, hook3.getPreStepHookCallCount());
        Assertions.assertEquals(1, hook3.getPostStepHookCallCount());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.RESULT_CODE, StepResult.SUCCEED_RESULT_CODE);

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.success(), executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostStepHook.class.getSimpleName(),
                "postStepHook", task.activityType().name())));
        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreAndPostStepHook.class.getSimpleName(),
                "postStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                StepResult.SUCCEED_RESULT_CODE)).intValue());
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

    @Test
    public void ignoresFailureInPreHook_HookRetryOnFailureDisabled() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPreHookThrowsExceptionNoRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreHookThrowsExceptionNoRetryOnFailure.class.getSimpleName(),
                "preStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void retryOnFailureInPreHook_HookRetryOnFailureEnabled() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPreHookThrowsExceptionRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertFalse(step.didThing()); // false because the pre-hook required a retry on failure, so the step never ran

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertNull(executor.getOutput()); // null because retry doesn't support output attributes, and there was no exception stack trace to record

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.ResultAction.RETRY, executor.getResult().getAction());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPreHookThrowsExceptionRetryOnFailure.class.getSimpleName(),
                "preStepHook", task.activityType().name())));
    }

    @Test
    public void ignoresFailureInPostHook_HookRetryOnFailureDisabled() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPostHookThrowsExceptionNoRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> fullOutput = new HashMap<>();
        fullOutput.putAll(input);
        fullOutput.putAll(output);
        fullOutput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        fullOutput.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(fullOutput, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostHookThrowsExceptionNoRetryOnFailure.class.getSimpleName(),
                "postStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void retryOnFailureInPostHook_HookRetryOnFailureEnabled() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        RuntimeException e = new RuntimeException("Wheeee!");
        WorkflowStepHook hook = new TestPostHookThrowsExceptionRetryOnFailure(e);

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step);
        builder.alwaysClose(step);
        builder.addStepHook(step, hook);

        Workflow workflow = new Workflow() {
            private final WorkflowGraph graph = builder.build();

            @Override
            public WorkflowGraph getGraph() {
                return graph;
            }
        };

        PollForActivityTaskResponse task = makeTask(input, STEP_NAME);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, workflow, step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing()); // true because we did the thing

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertNull(executor.getOutput()); // null because retry doesn't support output attributes, and there was no exception stack trace to record

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.ResultAction.RETRY, executor.getResult().getAction());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertTrue(fluxMetrics.getDurations().containsKey(WorkflowStepUtil.formatHookExecutionTimeName(TestPostHookThrowsExceptionRetryOnFailure.class.getSimpleName(),
                "postStepHook", task.activityType().name())));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    private PollForActivityTaskResponse makeTask(Map<String, String> input, String stepName) {
        return PollForActivityTaskResponse.builder()
                .activityId("some-activity-id")
                .activityType(ActivityType.builder().name(stepName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build())
                .taskToken(TASK_TOKEN)
                .input(StepAttributes.encode(input))
                .workflowExecution(WorkflowExecution.builder().workflowId("some-workflow-id").runId("run-id").build())
                .build();
    }

    private StepResult makeStepResult(StepResult.ResultAction resultAction, String stepResult, String message, Map<String, String> output) {
        return new StepResult(resultAction, stepResult, message).withAttributes(output);
    }

    public static class StubStep implements WorkflowStep {

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
                          @Attribute("ThisShouldBeNull") String nullParam,
                          @Attribute("ThisShouldBeEmpty") Map<String, String> emptyMapParam) {
            Assertions.assertNotNull(stringParam);
            Assertions.assertNotNull(longParam);
            Assertions.assertNotNull(booleanParam);
            Assertions.assertNotNull(stringMap);
            Assertions.assertNull(nullParam);
            Assertions.assertNotNull(emptyMapParam);

            Assertions.assertEquals(expectedString, stringParam);
            Assertions.assertEquals(expectedLong, longParam);
            Assertions.assertEquals(expectedBoolean, booleanParam);
            Assertions.assertEquals(expectedStringMap, stringMap);
            Assertions.assertTrue(emptyMapParam.isEmpty());
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

}
