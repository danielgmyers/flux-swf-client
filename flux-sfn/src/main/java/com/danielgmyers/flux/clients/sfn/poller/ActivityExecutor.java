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

import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;

/**
 * Executes the WorkflowStep associated with a specified ActivityTask.
 */
public class ActivityExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ActivityExecutor.class);

    // package-private for test visibility
    static final String WORKFLOW_ID_METRIC_NAME = "WorkflowId";
    static final String WORKFLOW_RUN_ID_METRIC_NAME = "RunId";
    static final String RETRY_CAUSE_FIELD_NAME = "RetryCause";

    private final String identity;
    private final GetActivityTaskResponse task;
    private final Workflow workflow;
    private final WorkflowStep step;
    private final MetricRecorder fluxMetrics;
    private final MetricRecorderFactory metricsFactory;

    private StepResult result;
    private String output;

    // package-private, only ActivityTaskPoller should be creating these
    ActivityExecutor(String identity, GetActivityTaskResponse task, Workflow workflow, WorkflowStep step,
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
        String activityName = TaskNaming.activityName(workflow, step);
        log.debug("Worker {} received activity task for activity {}.", identity, TaskNaming.activityName(workflow, step));

        try (MetricRecorder stepMetrics = metricsFactory.newMetricRecorder(activityName)) {
            SfnStepInputAccessor stepInput = new SfnStepInputAccessor(task.input());

            // In practice these two fields aren't meaningfully different for Step Functions, but we'll provide them both
            // since people might have code looking for either of them.
            stepMetrics.addProperty(WORKFLOW_ID_METRIC_NAME, stepInput.getAttribute(String.class, StepAttributes.WORKFLOW_ID));
            stepMetrics.addProperty(WORKFLOW_RUN_ID_METRIC_NAME,
                                    stepInput.getAttribute(String.class, StepAttributes.WORKFLOW_EXECUTION_ID));

            result = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, stepInput, fluxMetrics, stepMetrics);

            if (result.getAction() == StepResult.ResultAction.RETRY) {
                // If the retry was caused by an exception, record the stack trace of the exception in the output.
                if (result.getCause() != null) {
                    StringWriter sw = new StringWriter();
                    result.getCause().printStackTrace(new PrintWriter(sw));
                    stepInput.addAttribute(RETRY_CAUSE_FIELD_NAME, sw.toString());
                }
            } else {
                stepInput.addAttributes(result.getAttributes());
                stepInput.addAttribute(StepAttributes.RESULT_CODE, result.getResultCode());
                if (result.getMessage() != null && !result.getMessage().isEmpty()) {
                    stepInput.addAttribute(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
                }
            }

            output = stepInput.toJson();
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
