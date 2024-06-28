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

package com.danielgmyers.flux.clients.sfn.poller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.ActivityExecutionUtil;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.metrics.recorders.InMemoryMetricRecorder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ActivityExecutorTest {

    private static final String IDENTITY = "unit";
    private static final String WORKFLOW_NAME = "some-workflow-name";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DummyStep step;
    private InMemoryMetricRecorder fluxMetrics;
    private InMemoryMetricRecorder stepMetrics;

    @BeforeEach
    public void setup() {
        step = new DummyStep();
        fluxMetrics = new InMemoryMetricRecorder("ActivityExecutor");
        stepMetrics = new InMemoryMetricRecorder(TaskNaming.activityName(TestWorkflow.class, DummyStep.class));
    }

    @Test
    public void returnsCompleteForSuccessCase_IncludesInputInFinalOutputJson() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        input.put(StepAttributes.WORKFLOW_ID, WORKFLOW_NAME);
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, SfnArnFormatter.executionArn("us-west-2", "123456789012", TestWorkflow.class, WORKFLOW_NAME));
        input.put("starship", "enterprise");
        input.put("registry", 1701L);

        Map<String, Object> output = new HashMap<>();
        output.put("captain", "kirk");

        SfnStepInputAccessor stepInput = makeStepInput(input);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, stepInput, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.COMPLETE, StepResult.SUCCEED_RESULT_CODE, "yay", output);
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing());

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Map<String, Object> expectedOutputContent = new HashMap<>();
        expectedOutputContent.putAll(input);
        expectedOutputContent.putAll(output);
        expectedOutputContent.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
        expectedOutputContent.put(StepAttributes.RESULT_CODE, result.getResultCode());

        validateOutput(expectedOutputContent, executor.getOutput());

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_EXECUTION_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        String activityName = TaskNaming.activityName(TestWorkflow.class, DummyStep.class);
        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatCompletionResultMetricName(activityName,
                                result.getResultCode())).intValue());
    }

    @Test
    public void returnsRetryWhenApplyReturnsRetry() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        input.put(StepAttributes.WORKFLOW_ID, WORKFLOW_NAME);
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, SfnArnFormatter.executionArn("us-west-2", "123456789012", TestWorkflow.class, WORKFLOW_NAME));

        SfnStepInputAccessor stepInput = makeStepInput(input);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, stepInput, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

        StepResult result = makeStepResult(StepResult.ResultAction.RETRY, null, "hmm", Collections.emptyMap());
        step.setStepResult(result);

        executor.run();
        Assertions.assertTrue(step.didThing()); // true because the step did a thing before specifically deciding to return retry

        // fluxMetrics is usually closed by the ActivityTaskPoller.
        // We need to close it so we can query its data.
        fluxMetrics.close();

        Assertions.assertEquals(result.getMessage(), executor.getRetryCause());

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(result, executor.getResult());

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_EXECUTION_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        String activityName = TaskNaming.activityName(TestWorkflow.class, DummyStep.class);
        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(activityName,
                                null)).intValue());
    }

    @Test
    public void returnsRetryWhenApplyThrowsException() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        input.put(StepAttributes.WORKFLOW_ID, WORKFLOW_NAME);
        input.put(StepAttributes.WORKFLOW_EXECUTION_ID, SfnArnFormatter.executionArn("us-west-2", "123456789012", TestWorkflow.class, WORKFLOW_NAME));

        SfnStepInputAccessor stepInput = makeStepInput(input);
        ActivityExecutor executor = new ActivityExecutor(IDENTITY, stepInput, new TestWorkflow(), step, fluxMetrics,  (o, c) -> stepMetrics);

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

        Assertions.assertEquals(stackTrace, executor.getRetryCause());

        Assertions.assertNotNull(executor.getResult());
        Assertions.assertEquals(StepResult.ResultAction.RETRY, executor.getResult().getAction());

        Assertions.assertTrue(fluxMetrics.isClosed());
        Assertions.assertTrue(stepMetrics.isClosed());

        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_ID_METRIC_NAME));
        Assertions.assertEquals(input.get(StepAttributes.WORKFLOW_EXECUTION_ID), stepMetrics.getProperties().get(ActivityExecutor.WORKFLOW_RUN_ID_METRIC_NAME));

        String activityName = TaskNaming.activityName(TestWorkflow.class, DummyStep.class);
        Assertions.assertEquals(1, fluxMetrics.getCounts().get(ActivityExecutionUtil.formatRetryResultMetricName(activityName,
                                e.getClass().getSimpleName())).intValue());
    }

    private SfnStepInputAccessor makeStepInput(Map<String, Object> input) throws JsonProcessingException {
        return new SfnStepInputAccessor(MAPPER.writeValueAsString(input));
    }

    private StepResult makeStepResult(StepResult.ResultAction resultAction, String stepResult, String message, Map<String, Object> output) {
        return new StepResult(resultAction, stepResult, message).withAttributes(output);
    }

    private void validateOutput(Map<String, Object> expectedOutputFields, String actualOutput) throws JsonProcessingException {
        SfnStepInputAccessor outputAccessor = new SfnStepInputAccessor(actualOutput);

        for (Map.Entry<String, Object> f : expectedOutputFields.entrySet()) {
            Assertions.assertEquals(f.getValue(), outputAccessor.getAttribute(f.getValue().getClass(), f.getKey()));
        }
    }

    public class TestWorkflow implements Workflow {

        private final WorkflowGraph graph;

        public TestWorkflow() {
            WorkflowGraphBuilder graph = new WorkflowGraphBuilder(step);
            graph.alwaysClose(step);
            this.graph = graph.build();
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
}
