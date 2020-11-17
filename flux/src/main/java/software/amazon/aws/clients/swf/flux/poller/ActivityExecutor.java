/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.aws.clients.swf.flux.poller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepHook;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepHook;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepUtil;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.PostWorkflowHookAnchor;
import software.amazon.aws.clients.swf.flux.wf.graph.PreWorkflowHookAnchor;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse;

/**
 * Executes the WorkflowStep associated with a specified ActivityTask.
 */
public class ActivityExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ActivityExecutor.class);

    // package-private for test visibility
    static final String WORKFLOW_ID_METRIC_NAME = "WorkflowId";
    static final String WORKFLOW_RUN_ID_METRIC_NAME = "RunId";

    private final String identity;
    private final PollForActivityTaskResponse task;
    private final Workflow workflow;
    private final WorkflowStep step;
    private final MetricRecorder fluxMetrics;
    private final MetricRecorderFactory metricsFactory;

    private StepResult result;
    private String output;

    // package-private, only ActivityTaskPoller should be creating these
    ActivityExecutor(String identity, PollForActivityTaskResponse task, Workflow workflow, WorkflowStep step,
                     MetricRecorder fluxMetrics, MetricRecorderFactory metricsFactory) {
        this.identity = identity;
        this.task = task;
        this.workflow = workflow;
        this.step = step;
        this.fluxMetrics = fluxMetrics;
        this.metricsFactory = metricsFactory;
        this.result = null;
        this.output = null;
    }

    public StepResult getResult() {
        return result;
    }

    public String getOutput() {
        return output;
    }

    @Override
    public void run() {
        log.debug("Worker {} received activity task for activity {} id {}.", identity, task.activityType().name(),
            task.activityId());

        String workflowId = task.workflowExecution().workflowId();
        String runId = task.workflowExecution().runId();

        try (MetricRecorder stepMetrics = metricsFactory.newMetricRecorder(task.activityType().name());
            MDC.MDCCloseable ignored = MDC.putCloseable(WORKFLOW_ID_METRIC_NAME, workflowId);
            MDC.MDCCloseable ignored1 = MDC.putCloseable(WORKFLOW_RUN_ID_METRIC_NAME, runId)
        ) {
            stepMetrics.addProperty(WORKFLOW_ID_METRIC_NAME, workflowId);
            stepMetrics.addProperty(WORKFLOW_RUN_ID_METRIC_NAME, runId);

            Map<String, String> input = StepAttributes.decode(Map.class, task.input());

            List<WorkflowStepHook> hooks = workflow.getGraph().getHooksForStep(step.getClass());

            Map<String, String> hookInput = new HashMap<>(input);
            hookInput.put(StepAttributes.ACTIVITY_NAME, StepAttributes.encode(task.activityType().name()));

            if (hooks != null && step.getClass() != PostWorkflowHookAnchor.class) {
                result = WorkflowStepUtil.executeHooks(hooks, hookInput, StepHook.HookType.PRE, task.activityType().name(),
                        fluxMetrics, stepMetrics);
            }

            Map<String, String> outputAttributes = new HashMap<>(input);

            if (result == null) {
                result = executeActivity(step, task.activityType().name(), fluxMetrics, stepMetrics, input);

                // yes, this means the output attributes are serialized into a map, which is itself serialized.
                // this makes deserialization less confusing later because we can deserialize as a map of strings
                // and then deserialize each value as a specific type.
                Map<String, String> serializedResultAttributes = StepAttributes.serializeMapValues(result.getAttributes());
                outputAttributes.putAll(serializedResultAttributes);
                hookInput.putAll(serializedResultAttributes);

                // retries put their reason message in the special ActivityTaskFailed reason field.
                if (result.getAction() != StepResult.ResultAction.RETRY && result.getMessage() != null) {
                    outputAttributes.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
                    hookInput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, StepAttributes.encode(result.getMessage()));
                }
                if (result.getResultCode() != null) {
                    outputAttributes.put(StepAttributes.RESULT_CODE, result.getResultCode());
                    hookInput.put(StepAttributes.RESULT_CODE, StepAttributes.encode(result.getResultCode()));
                }

                if (hooks != null && step.getClass() != PreWorkflowHookAnchor.class) {
                    StepResult hookResult = WorkflowStepUtil.executeHooks(hooks, hookInput, StepHook.HookType.POST,
                        task.activityType().name(), fluxMetrics, stepMetrics);
                    if (hookResult != null) {
                        log.info("Activity {} returned result {} ({}) but a post-step hook requires a retry ({}).",
                            task.activityType().name(), result.getResultCode(), result.getMessage(),
                            hookResult.getMessage());
                        result = hookResult;
                        outputAttributes.remove(StepAttributes.ACTIVITY_COMPLETION_MESSAGE);
                        outputAttributes.remove(StepAttributes.RESULT_CODE);
                    }
                }
            }

            if (result != null && result.getAction() == StepResult.ResultAction.RETRY) {
                // for retries, we'll check if the retry was caused by an exception and, if so,
                // record the stack trace of the exception in the output.
                if (result.getCause() != null) {
                    StringWriter sw = new StringWriter();
                    result.getCause().printStackTrace(new PrintWriter(sw));
                    output = sw.toString();
                } else {
                    // otherwise, the output should be blank. It will be ignored anyway.
                    output = null;
                }
            } else {
                output = StepAttributes.encode(outputAttributes);
            }
        } catch (RuntimeException e) {
            // we've suppressed java.lang.Thread's default behavior (print to stdout), so we want the error logged.
            log.error("Caught an exception while executing activity task", e);
            throw e;
        }
    }

    /**
     * Executes the specified workflow step. Converts any exception returned from the step into a retry.
     *
     * This method is public static for use in StepValidator to ensure people get the exact same logic in unit test validation.
     *
     * @param step The step to execute
     * @param activityName The full activity name (as determined by TaskNaming.activityName) for the activity being executed.
     * @param fluxMetrics A Metrics object to use to report Flux framework fluxMetrics.
     * @param stepMetrics A fluxMetrics object steps can request for reporting their own fluxMetrics.
     * @param input The input to pass to the step
     * @return The result of the step execution.
      */
    public static StepResult executeActivity(WorkflowStep step, String activityName, MetricRecorder fluxMetrics,
                                             MetricRecorder stepMetrics, Map<String, String> input) {
        StepResult result;
        String retryExceptionCauseName = null;
        try {
            Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
            Object returnObject = applyMethod.invoke(step, WorkflowStepUtil.generateArguments(step.getClass(), applyMethod,
                                                                                              stepMetrics, input));
            // if apply didn't throw an exception, then we can assume success.
            // However, if the apply method's return type is StepResult, then we need to respect it.
            result = StepResult.success();
            if (applyMethod.getReturnType() == StepResult.class && returnObject != null) {
                result = (StepResult)returnObject;
            }
        } catch (InvocationTargetException e) { // ITE when invoke's target (the step) throws an exception
            // All exceptions should result in a retry.  We can get the activity's exception in the cause field of the ITE.
            retryExceptionCauseName = e.getCause().getClass().getSimpleName();
            result = StepResult.retry(e.getCause());
            log.info("Step {} threw an exception ({}), returning a retry result.",
                     step.getClass().getSimpleName(), e.getCause().toString(),
                     e.getCause());
        } catch (IllegalAccessException e) {
            // IllegalAccessException shouldn't happen, since we only looked for public methods,
            // but if it does happen we'll just let the workflow retry the step.
            retryExceptionCauseName = e.getClass().getSimpleName();
            result = StepResult.retry(e);
            log.error("Got an exception ({}) attempting to execute step {}, returning a retry result.",
                      e.getMessage(), step.getClass().getSimpleName(), e);
        }

        // now figure out which fluxMetrics to emit
        if (retryExceptionCauseName != null) {
            fluxMetrics.addCount(formatRetryResultMetricName(activityName, retryExceptionCauseName), 1.0);
        } else if (result.getAction() == StepResult.ResultAction.RETRY) {
            fluxMetrics.addCount(formatRetryResultMetricName(activityName, null), 1.0);
        } else {
            fluxMetrics.addCount(formatCompletionResultMetricName(activityName, result.getResultCode()), 1.0);
        }

        return result;
    }

    // package-private for test visibility
    // pass null for retryExceptionCauseName if the retry wasn't caused by an exception
    static String formatRetryResultMetricName(String activityName, String retryExceptionCauseName) {
        if (retryExceptionCauseName == null) {
            return String.format("Flux.ActivityResult.%s.Retry", activityName);
        } else {
            return String.format("Flux.ActivityResult.%s.Retry.%s", activityName, retryExceptionCauseName);
        }
    }

    // package-private for testing visibility
    static String formatCompletionResultMetricName(String activityName, String completionResultCode) {
        return String.format("Flux.ActivityResult.%s.CompletionCode.%s", activityName, completionResultCode);
    }
}
