package software.amazon.aws.clients.swf.flux.poller;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.step.WorkflowStepUtil;

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
