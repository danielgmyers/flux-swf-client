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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.util.RetryUtils;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskRequest;
import software.amazon.awssdk.services.swf.model.PollForActivityTaskResponse;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatRequest;
import software.amazon.awssdk.services.swf.model.RecordActivityTaskHeartbeatResponse;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCanceledRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.RespondActivityTaskFailedRequest;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;

/**
 * Poller that requests and handles activity tasks.
 */
public class ActivityTaskPoller implements Runnable {

    // package-private for test visibility
    static final String ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX = "Flux.ActivityTaskPoll";
    static final String RESPOND_ACTIVITY_TASK_COMPLETED_METRIC_PREFIX = "Flux.RespondActivityTaskCompleted";
    static final String RESPOND_ACTIVITY_TASK_FAILED_METRIC_PREFIX = "Flux.RespondActivityTaskSucceeded";

    static final String WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME = "Flux.WorkerThreadAvailabilityWaitTime";
    static final String NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME = "Flux.NoActivityTaskToExecute";

    // package-private for unit test access
    static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(10);
    static final String SUFFIX_INDICATING_TRUNCATION = " <truncated>";

    private final Logger log = LoggerFactory.getLogger(ActivityTaskPoller.class);

    private final MetricRecorderFactory metricsFactory;
    private final SwfClient swf;
    private final String domain;
    private final String taskListName;
    private final String identity;

    private final Map<String, Workflow> workflows;
    private final Map<String, WorkflowStep> workflowSteps;

    private final BlockOnSubmissionThreadPoolExecutor workerThreadPool;

    /**
     * Constructs an activity poller.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param swfClient An already-configured SWF client to be used for polling.
     * @param workflowDomain The workflow domain that should be polled for tasks.
     * @param taskListName The task list that should be polled for tasks.
     * @param identity The worker identity that the poller should report to SWF for this poller.
     * @param workflows A map of workflow names to Workflow objects.
     * @param workflowSteps A map of workflow step names to workflow steps to be used by the activity execution logic.
     * @param workerThreadPool The pool of threads available to hand activity tasks off to.
     */
    public ActivityTaskPoller(MetricRecorderFactory metricsFactory, SwfClient swfClient, String workflowDomain,
                              String taskListName, String identity, Map<String, Workflow> workflows,
                              Map<String, WorkflowStep> workflowSteps, BlockOnSubmissionThreadPoolExecutor workerThreadPool) {
        this.metricsFactory = metricsFactory;
        this.swf = swfClient;
        this.domain = workflowDomain;
        this.taskListName = taskListName;

        if (identity == null || identity.length() <= 0 || identity.length() > 256) {
            throw new IllegalArgumentException("Invalid identity for task poller, must be 1-256 characters: " + identity);
        }
        this.identity = identity;

        this.workflows = workflows;
        this.workflowSteps = workflowSteps;

        this.workerThreadPool = workerThreadPool;
    }

    @Override
    public void run() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName())) {
            metrics.startDuration(WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
            workerThreadPool.execute(() -> {
                Duration waitTime = metrics.endDuration(WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
                // emit the wait time metric again, under this poller's task list name.
                metrics.addDuration(WORKER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + taskListName, waitTime);
                pollForActivityTask(metrics);
            });
        } catch (RejectedExecutionException e) {
            // the activity task will time out in this case, so another host will get assigned to it.
            log.warn("The activity thread pool rejected the task. This is usually because it is shutting down.", e);
        } catch (Throwable t) {
            log.debug("Got exception while polling for or executing activity task", t);
            throw t;
        }
    }

    private Runnable pollForActivityTask(MetricRecorder metrics) {
        PollForActivityTaskRequest request = PollForActivityTaskRequest.builder()
                .domain(domain).taskList(TaskList.builder().name(taskListName).build()).identity(identity).build();

        PollForActivityTaskResponse task
                = RetryUtils.executeWithInlineBackoff(() -> swf.pollForActivityTask(request),
                                                      20, Duration.ofSeconds(2), metrics,
                                                      ACTIVITY_TASK_POLL_TIME_METRIC_PREFIX);

        if (task == null || task.taskToken() == null || task.taskToken().equals("")) {
            // this means there was no work to do
            metrics.addCount(NO_ACTIVITY_TASK_TO_EXECUTE_METRIC_NAME, 1.0);
            return null;
        }

        WorkflowStep step = workflowSteps.get(task.activityType().name());
        if (step == null) {
            metrics.addCount(formatUnrecognizedActivityTaskMetricName(task.activityType().name()), 1.0);
            String message = "Activity task received for unrecognized activity: " + task.activityType().name();
            log.warn(message);
            throw new UnrecognizedTaskException(message);
        }

        Workflow workflow = workflows.get(TaskNaming.workflowNameFromActivityName(task.activityType().name()));
        if (workflow == null) {
            String message = "Activity " + task.activityType().name()
                             + " was a valid activity but somehow not a valid workflow, this should not be possible!";
            log.error(message);
            throw new IllegalStateException(message);
        }

        log.debug("Polled for activity task and there was work to do.");
        return () -> executeWithHeartbeat(task, workflow, step);
    }

    private void executeWithHeartbeat(PollForActivityTaskResponse task, Workflow workflow, WorkflowStep step) {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName()
                                                                       + ".executeWithHeartbeat")) {
            String activityExecutionTimeMetricName = formatActivityExecutionTimeMetricName(task.activityType().name());
            metrics.startDuration(activityExecutionTimeMetricName);
            ActivityExecutor executor = new ActivityExecutor(identity, task, workflow, step, metrics, metricsFactory);

            Thread activityThread = new Thread(executor);
            activityThread.start();

            boolean canceled = false;

            try {
                while (true) {
                    activityThread.join(HEARTBEAT_INTERVAL.toMillis());
                    if (!activityThread.isAlive()) {
                        log.debug("The activity thread has ended successfully.");
                        break;
                    }

                    try {
                        RecordActivityTaskHeartbeatRequest request
                                = RecordActivityTaskHeartbeatRequest.builder().taskToken(task.taskToken()).build();
                        log.debug("Sending heartbeat for workflow id {} at step {} with activity id {}.",
                                  task.workflowExecution().workflowId(), TaskNaming.activityName(workflow, step),
                                  task.activityId());
                        RecordActivityTaskHeartbeatResponse status = swf.recordActivityTaskHeartbeat(request);
                        log.debug("Recorded heartbeat for workflow id {} at step {} with activity id {}.",
                                  task.workflowExecution().workflowId(), TaskNaming.activityName(workflow, step),
                                  task.activityId());
                        if (status.cancelRequested()) {
                            log.warn("The heartbeat told us that we received a cancellation request for this task.");
                            canceled = true;
                        }
                    } catch (UnknownResourceException e) {
                        // If the resource (e.g. the activity task) no longer exists,
                        // then there's no reason to continue it, since we can't report success or failure.
                        // If this happens, usually it means the task timed out in SWF
                        // before we managed to send a heartbeat.
                        log.warn("The heartbeat told us that the resource no longer exists, so we'll cancel this task."
                                 + " {}", e.getMessage());
                        canceled = true;
                    } catch (Exception e) {
                        log.warn("Got an error while trying to record a heartbeat.", e);
                        // If we weren't able to heartbeat, we don't want to fail.
                        // It may have been transient and we'll make another attempt after HEARTBEAT_INTERVAL has passed.
                        // If it fails enough for the activity to time out, oh well...
                    }
                    if (canceled) {
                        // If a cancel was requested for the activity, we kill the task and bail out.
                        activityThread.interrupt();
                        activityThread.join(); // we interrupted it, so we'll wait for it to end
                    }
                }
            } catch (InterruptedException e) {
                metrics.addCount(formatActivityThreadInterruptedMetricName(task.activityType().name()), 1.0);
                // We don't really know what happened in this case, we'll use e.getCause() if it's not null, otherwise e.
                Throwable cause = (e.getCause() == null ? e : e.getCause());
                String msg = "Error executing activity task";
                log.warn(msg, cause);
                throw new RuntimeException(msg, cause);
            }

            metrics.endDuration(activityExecutionTimeMetricName);

            // at this point the executor should be done.
            if (canceled) {
                metrics.addCount(formatActivityTaskCancelledMetricName(task.activityType().name()), 1.0);
                // if we were cancelled, report that we successfully cancelled the step.
                swf.respondActivityTaskCanceled(RespondActivityTaskCanceledRequest.builder().taskToken(task.taskToken()).build());
            } else {
                StepResult result = executor.getResult();

                switch (result.getAction()) {
                    case COMPLETE:
                        RespondActivityTaskCompletedRequest rac = RespondActivityTaskCompletedRequest.builder()
                                .taskToken(task.taskToken()).result(executor.getOutput()).build();
                        RetryUtils.executeWithInlineBackoff(() -> swf.respondActivityTaskCompleted(rac),
                                                            20, Duration.ofSeconds(2), metrics,
                                                            RESPOND_ACTIVITY_TASK_COMPLETED_METRIC_PREFIX);
                        break;
                    case RETRY:
                        // SWF doesn't model retries, so we model it as a failure here.
                        // The output from the executor is included here,
                        // as it may contain the stack trace for the exception that caused the retry.
                        RespondActivityTaskFailedRequest raf = RespondActivityTaskFailedRequest.builder()
                                .taskToken(task.taskToken()).reason(prepareRetryReason(result.getMessage()))
                                .details(prepareRetryDetails(executor.getOutput())).build();
                        RetryUtils.executeWithInlineBackoff(() -> swf.respondActivityTaskFailed(raf),
                                                            20, Duration.ofSeconds(2), metrics,
                                                            RESPOND_ACTIVITY_TASK_FAILED_METRIC_PREFIX);
                        break;
                    default:
                        throw new RuntimeException("Unknown result action: " + result.getAction());
                }
            }
        }
    }

    // package-private for unit testing
    String prepareRetryReason(String reason) {
        // max length of the reason field in SWF is 256 characters.
        if (reason != null && reason.length() > 256) {
            log.warn("Result reason will be truncated in the response to SWF: {}", reason);
            int len = 256 - SUFFIX_INDICATING_TRUNCATION.length();
            return reason.substring(0, len) + SUFFIX_INDICATING_TRUNCATION;
        }
        return reason;
    }

    // package-private for unit testing
    String prepareRetryDetails(String details) {
        // max length of the details field in SWF is 32768 characters.
        if (details != null && details.length() > 32768) {
            log.warn("Result details will be truncated in the response to SWF: {}", details);
            int len = 32768 - SUFFIX_INDICATING_TRUNCATION.length();
            return details.substring(0, len) + SUFFIX_INDICATING_TRUNCATION;
        }
        return details;
    }

    // package-private for test visibility
    static String formatUnrecognizedActivityTaskMetricName(String activityName) {
        return String.format("Flux.UnrecognizedActivityTask.%s", activityName);
    }

    // package-private for test visibility
    static String formatActivityThreadInterruptedMetricName(String activityName) {
        return String.format("Flux.ActivityThreadInterrupted.%s", activityName);
    }

    // package-private for test visibility
    static String formatActivityExecutionTimeMetricName(String activityName) {
        return String.format("Flux.ActivityExecutionTime.%s", activityName);
    }

    // package-private for test visibility
    static String formatActivityTaskCancelledMetricName(String activityName) {
        return String.format("Flux.ActivityTaskCancelled.%s", activityName);
    }

}
