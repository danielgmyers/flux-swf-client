package com.danielgmyers.flux.step.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.poller.TaskNaming;
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
import com.danielgmyers.metrics.MetricRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ActivityExecutionUtil {

    private ActivityExecutionUtil() {}

    private static final Logger log = LoggerFactory.getLogger(ActivityExecutionUtil.class);

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
                                             MetricRecorder stepMetrics, StepInputAccessor input) {
        StepResult result;
        String retryExceptionCauseName = null;
        try {
            Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
            Object returnObject = applyMethod.invoke(step, WorkflowStepUtil.generateArguments(step.getClass(), applyMethod,
                                                                                              stepMetrics, input,
                                                                                              Collections.emptyMap()));
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

    public static StepResult executeHooksAndActivity(Workflow workflow, WorkflowStep step, StepInputAccessor stepInput,
                                                     MetricRecorder fluxMetrics, MetricRecorder stepMetrics) {
        String activityName = TaskNaming.activityName(workflow, step);
        try {
            List<WorkflowStepHook> hooks = workflow.getGraph().getHooksForStep(step.getClass());

            Map<String, Object> hookInput = new HashMap<>();
            hookInput.put(StepAttributes.ACTIVITY_NAME, StepAttributes.encode(activityName));

            StepResult result = null;
            if (hooks != null && step.getClass() != PostWorkflowHookAnchor.class) {
                result = WorkflowStepUtil.executeHooks(hooks, stepInput, hookInput, StepHook.HookType.PRE, activityName,
                        fluxMetrics, stepMetrics);
            }

            if (result == null) {
                result = executeActivity(step, activityName, fluxMetrics, stepMetrics, stepInput);

                hookInput.putAll(result.getAttributes());

                // retries put their reason message in the special ActivityTaskFailed reason field.
                if (result.getAction() != StepResult.ResultAction.RETRY && result.getMessage() != null) {
                    hookInput.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
                }
                if (result.getResultCode() != null) {
                    hookInput.put(StepAttributes.RESULT_CODE, result.getResultCode());
                }

                if (hooks != null && step.getClass() != PreWorkflowHookAnchor.class) {
                    StepResult hookResult = WorkflowStepUtil.executeHooks(hooks, stepInput, hookInput, StepHook.HookType.POST,
                            activityName, fluxMetrics, stepMetrics);
                    if (hookResult != null) {
                        log.info("Activity {} returned result {} ({}) but a post-step hook requires a retry ({}).",
                                activityName, result.getResultCode(), result.getMessage(),
                                hookResult.getMessage());
                        return hookResult;
                    }
                }
            }

            return result;
        } catch (RuntimeException e) {
            // we've suppressed java.lang.Thread's default behavior (print to stdout), so we want the error logged.
            log.error("Caught an exception while executing activity task", e);
            throw e;
        }
    }

    // public for test visibility
    // pass null for retryExceptionCauseName if the retry wasn't caused by an exception
    public static String formatRetryResultMetricName(String activityName, String retryExceptionCauseName) {
        if (retryExceptionCauseName == null) {
            return String.format("Flux.ActivityResult.%s.Retry", activityName);
        } else {
            return String.format("Flux.ActivityResult.%s.Retry.%s", activityName, retryExceptionCauseName);
        }
    }

    // public for testing visibility
    public static String formatCompletionResultMetricName(String activityName, String completionResultCode) {
        return String.format("Flux.ActivityResult.%s.CompletionCode.%s", activityName, completionResultCode);
    }
}
