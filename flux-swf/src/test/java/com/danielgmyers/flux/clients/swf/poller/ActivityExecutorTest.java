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
import com.danielgmyers.flux.clients.swf.poller.testwf.TestWorkflow;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.internal.ActivityExecutionUtil;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;

public class ActivityExecutorTest {

    private static final String IDENTITY = "unit";
    private static final String TASK_TOKEN = "task-token";

    private ActivityTaskPollerTest.DummyStep step;
    private InMemoryMetricRecorder fluxMetrics;
    private InMemoryMetricRecorder stepMetrics;

    @BeforeEach
    public void setup() {
        step = new ActivityTaskPollerTest.DummyStep();
        fluxMetrics = new InMemoryMetricRecorder("ActivityExecutor");
        stepMetrics = new InMemoryMetricRecorder(TaskNaming.activityName(TestWorkflow.class, ActivityTaskPollerTest.DummyStep.class));
    }

    @Test
    public void returnsCompleteForSuccessCase() {
        Map<String, String> input = new HashMap<>();
        Map<String, String> output = Collections.emptyMap();

        String activityName = TaskNaming.activityName(TestWorkflow.class, ActivityTaskPollerTest.DummyStep.class);
        PollForActivityTaskResponse task = makeTask(input, activityName);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, task, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, String> expectedOutputContent = new HashMap<>();
        expectedOutputContent.putAll(input);
        expectedOutputContent.putAll(output);
        expectedOutputContent.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        expectedOutputContent.put(StepAttributes.RESULT_CODE, result.getResultCode());

        Assertions.assertNotNull(executor.getOutput());
        Assertions.assertEquals(expectedOutputContent, StepAttributes.decode(Map.class, executor.getOutput()));

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(task.activityType().name(),
                                result.getResultCode())).intValue());
    }

    @Test
    public void returnsRetryWhenApplyReturnsRetry() {
        Map<String, String> input = new HashMap<>();

        String activityName = TaskNaming.activityName(TestWorkflow.class, ActivityTaskPollerTest.DummyStep.class);
        PollForActivityTaskResponse task = makeTask(input, activityName);
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

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(),
                                null)).intValue());
    }

    @Test
    public void returnsRetryWhenApplyThrowsException() {
        Map<String, String> input = new HashMap<>();

        String activityName = TaskNaming.activityName(TestWorkflow.class, ActivityTaskPollerTest.DummyStep.class);
        PollForActivityTaskResponse task = makeTask(input, activityName);
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

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(task.workflowExecution().workflowId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(task.workflowExecution().runId(), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(task.activityType().name(),
                                e.getClass().getSimpleName())).intValue());
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

}
