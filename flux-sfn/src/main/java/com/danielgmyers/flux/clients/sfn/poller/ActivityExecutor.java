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

import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.ActivityExecutionUtil;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the WorkflowStep associated with a specified ActivityTask.
 */
public class ActivityExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ActivityExecutor.class);

    // package-private for test visibility
    static final String WORKFLOW_ID_METRIC_NAME = "WorkflowId";
    static final String WORKFLOW_RUN_ID_METRIC_NAME = "RunId";

    private final String identity;
    private final SfnStepInputAccessor stepInput;
    private final Workflow workflow;
    private final WorkflowStep step;
    private final MetricRecorder fluxMetrics;
    private final MetricRecorderFactory metricsFactory;

    private StepResult result;
    private String output;
    private String retryCause;

    // package-private, only ActivityTaskPoller should be creating these
    ActivityExecutor(String identity, SfnStepInputAccessor stepInput, Workflow workflow, WorkflowStep step,
                     MetricRecorder fluxMetrics, MetricRecorderFactory metricsFactory) {
        this.identity = identity;
        this.stepInput = stepInput;
        this.workflow = workflow;
        this.step = step;
        this.fluxMetrics = fluxMetrics;
        this.metricsFactory = metricsFactory;
        this.result = null;
        this.output = null;
        this.retryCause = null;
    }

    public StepResult getResult() {
        return result;
    }

    public String getOutput() {
        return output;
    }

    public String getRetryCause() {
        return retryCause;
    }

    @Override
    public void run() {
        String activityName = TaskNaming.activityName(workflow, step);
        log.debug("Worker {} received activity task for activity {}.", identity, TaskNaming.activityName(workflow, step));

        try (MetricRecorder stepMetrics = metricsFactory.newMetricRecorder(activityName)) {
            // In practice these two fields aren't meaningfully different for Step Functions, but we'll provide them both
            // since people might have code looking for either of them.
            stepMetrics.addProperty(WORKFLOW_ID_METRIC_NAME, stepInput.getAttribute(String.class, StepAttributes.WORKFLOW_ID));
            stepMetrics.addProperty(WORKFLOW_RUN_ID_METRIC_NAME,
                                    stepInput.getAttribute(String.class, StepAttributes.WORKFLOW_EXECUTION_ID));

            result = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, stepInput, fluxMetrics, stepMetrics);

            if (result.getAction() == StepResult.ResultAction.RETRY) {
                // If the retry was caused by an exception, record the stack trace of the exception in the output.
                // Otherwise, record the user-provided message, if any.
                if (result.getCause() != null) {
                    StringWriter sw = new StringWriter();
                    result.getCause().printStackTrace(new PrintWriter(sw));
                    retryCause = sw.toString();
                } else if (result.getMessage() != null && !result.getMessage().isEmpty()) {
                    retryCause = result.getMessage();
                }
            } else {
                stepInput.addAttributes(result.getAttributes());
                stepInput.addAttribute(StepAttributes.RESULT_CODE, result.getResultCode());
                if (result.getMessage() != null && !result.getMessage().isEmpty()) {
                    stepInput.addAttribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
                }

                // Step Functions doesn't have a way to auto-merge the output data with the input data,
                // so we need to include both input and output attributes in the output field here.
                output = stepInput.toJson();
            }
        } catch (JsonProcessingException e) {
            // we've suppressed java.lang.Thread's default behavior (print to stdout), so we want the error logged.
            String message = "Unable to parse activity input or output as json";
            log.error(message, e);
            throw new FluxException(message, e);
        } catch (RuntimeException e) {
            // we've suppressed java.lang.Thread's default behavior (print to stdout), so we want the error logged.
            log.error("Caught an exception while executing activity task", e);
            throw e;
        }
    }
}
