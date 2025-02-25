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
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.step.SwfStepAttributeManager;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.ActivityExecutionUtil;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

            SwfStepAttributeManager stepInput = new SwfStepAttributeManager(task.input());

            result = ActivityExecutionUtil.executeHooksAndActivity(workflow, step, stepInput, fluxMetrics, stepMetrics);
            if (result.getAction() == StepResult.ResultAction.RETRY) {
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
                Map<String, String> combinedAttributes = new HashMap<>(stepInput.getEncodedAttributes());
                for (Map.Entry<String, Object> outputAttribute : result.getAttributes().entrySet()) {
                    combinedAttributes.put(outputAttribute.getKey(), StepAttributes.encode(outputAttribute.getValue()));
                }
                // These two output attributes are specifically _not_ double-encoded, which means unfortunately
                // they're not consistent with the rest of the output attributes.
                combinedAttributes.put(StepAttributes.RESULT_CODE, result.getResultCode());
                if (result.getMessage() != null && !result.getMessage().isEmpty()) {
                    combinedAttributes.put(StepAttributes.ACTIVITY_COMPLETION_MESSAGE, result.getMessage());
                }
                output = StepAttributes.encode(combinedAttributes);
            }
        } catch (RuntimeException e) {
            // we've suppressed java.lang.Thread's default behavior (print to stdout), so we want the error logged.
            log.error("Caught an exception while executing activity task", e);
            throw e;
        }
    }
}
