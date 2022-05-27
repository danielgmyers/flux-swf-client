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

package software.amazon.aws.clients.swf.flux.poller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
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
                result = ActivityExecutionUtil.executeActivity(step, task.activityType().name(), fluxMetrics, stepMetrics, input);

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
}
