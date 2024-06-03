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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;

import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.clients.swf.IdentifierValidation;
import com.danielgmyers.flux.clients.swf.poller.timers.TimerData;
import com.danielgmyers.flux.clients.swf.util.RetryUtils;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.signals.BaseSignalData;
import com.danielgmyers.flux.signals.DelayRetrySignalData;
import com.danielgmyers.flux.signals.ScheduleDelayedRetrySignalData;
import com.danielgmyers.flux.signals.SignalType;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.WorkflowStepUtil;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.threads.ThreadUtils;
import com.danielgmyers.flux.wf.Periodic;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.ActivityType;
import software.amazon.awssdk.services.swf.model.CancelTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.CancelWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.CompleteWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.ContinueAsNewWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.Decision;
import software.amazon.awssdk.services.swf.model.DecisionType;
import software.amazon.awssdk.services.swf.model.FailWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.HistoryEvent;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskRequest;
import software.amazon.awssdk.services.swf.model.PollForDecisionTaskResponse;
import software.amazon.awssdk.services.swf.model.RecordMarkerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.RequestCancelActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.RespondDecisionTaskCompletedRequest;
import software.amazon.awssdk.services.swf.model.ScheduleActivityTaskDecisionAttributes;
import software.amazon.awssdk.services.swf.model.SignalExternalWorkflowExecutionDecisionAttributes;
import software.amazon.awssdk.services.swf.model.StartTimerDecisionAttributes;
import software.amazon.awssdk.services.swf.model.TaskList;

/**
 * Poller that requests and handles decision tasks.
 */
public class DecisionTaskPoller implements Runnable {

    // package-private for test visibility
    static final String DECISION_TASK_POLL_TIME_METRIC_PREFIX = "Flux.DecisionTaskPoll";
    static final String DECISION_TASK_EVENT_HISTORY_LOOKUP_TIME_METRIC_PREFIX = "Flux.DecisionTaskEventHistoryLookup";
    static final String RESPOND_DECISION_TASK_COMPLETED_METRIC_PREFIX = "Flux.RespondDecisionTaskCompleted";

    static final String DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME = "Flux.DeciderThreadAvailabilityWaitTime";
    static final String NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME = "Flux.NoDecisionTaskToExecute";

    static final String WORKFLOW_ID_METRIC_NAME = "WorkflowId";
    static final String WORKFLOW_RUN_ID_METRIC_NAME = "RunId";

    static final String UNKNOWN_RESULT_CODE_METRIC_BASE = "Flux.UnknownResultCode";

    static final String DELAY_EXIT_TIMER_ID = "_delayExit";
    static final String UNKNOWN_RESULT_RETRY_TIMER_ID = "_unknownResultCode";
    static final String FORCE_NEW_DECISION_TIMER_ID = "_forceNewDecision";

    static final Duration UNKNOWN_RESULT_RETRY_TIMER_DELAY = Duration.ofMinutes(1);

    private static final Logger log = LoggerFactory.getLogger(DecisionTaskPoller.class);

    private final MetricRecorderFactory metricsFactory;
    private final SwfClient swf;
    private final String domain;
    private final String taskListName;
    private final String identity;
    private final double exponentialBackoffBase;

    private final Map<String, Workflow> workflows;
    private final Map<String, WorkflowStep> workflowSteps;

    private final BlockOnSubmissionThreadPoolExecutor deciderThreadPool;
    private final FluxCapacitorConfig config;

    private final Clock clock;

    /**
     * Constructs a decision poller.
     *
     * @param metricsFactory A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param swfClient      An already-configured SWF client to be used for polling.
     * @param config         The FluxCapacitorConfig containing runtime configuration for Flux.
     * @param taskListName   The task list that should be polled for tasks.
     * @param identity       The worker identity that the poller should report to SWF for this poller.
     * @param workflows      A map of workflow names to workflow objects to be used by the decision logic.
     * @param workflowSteps  A map of workflow step names to WorkflowStep objects to be used by the decision logic.
     * @param deciderThreadPool The pool of threads available to hand decision tasks off to.
     */
    public DecisionTaskPoller(MetricRecorderFactory metricsFactory, SwfClient swfClient, FluxCapacitorConfig config,
                              String taskListName, String identity,
                              Map<String, Workflow> workflows, Map<String, WorkflowStep> workflowSteps,
                              BlockOnSubmissionThreadPoolExecutor deciderThreadPool, Clock clock) {
        this.metricsFactory = metricsFactory;
        this.swf = swfClient;
        this.domain = config.getSwfDomain();
        this.taskListName = taskListName;

        if (identity == null || identity.length() <= 0 || identity.length() > 256) {
            throw new IllegalArgumentException("Invalid identity for task poller, must be 1-256 characters: " + identity);
        }
        this.identity = identity;

        this.exponentialBackoffBase = (config.getExponentialBackoffBase() != null
                                       ? config.getExponentialBackoffBase()
                                       : FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);

        this.workflows = workflows;
        this.workflowSteps = workflowSteps;

        this.deciderThreadPool = deciderThreadPool;

        this.config = config;

        this.clock = clock;
    }

    @Override
    public void run() {
        // not using try-with-resources because the metrics context needs to get closed after the poller thread
        // gets executed, rather than when this method returns.
        MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName());
        try {
            metrics.startDuration(DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
            deciderThreadPool.executeWhenCapacityAvailable(() -> pollForDecisionTask(metrics));
        } catch (RejectedExecutionException e) {
            // the decision task will time out in this case, so another host will get assigned to it.
            log.warn("The decider thread pool rejected the task. This is usually because it is shutting down.", e);
        } catch (Throwable t) {
            log.debug("Got exception while polling for or executing decision task", t);
            throw t;
        }
    }

    private Runnable pollForDecisionTask(MetricRecorder metrics) {
        try (metrics) {
            Duration waitTime = metrics.endDuration(DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME);
            // emit the wait time metric again, under this poller's task list name.
            metrics.addDuration(DECIDER_THREAD_AVAILABILITY_WAIT_TIME_METRIC_NAME + "." + taskListName, waitTime);

            PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder()
                    .domain(domain)
                    .taskList(TaskList.builder().name(taskListName).build())
                    .identity(identity)
                    .reverseOrder(true)
                    .build();

            log.debug("Polling for decision task");
            PollForDecisionTaskResponse task
                    = RetryUtils.executeWithInlineBackoff(() -> swf.pollForDecisionTask(request),
                                                          20, Duration.ofSeconds(2), metrics,
                                                          DECISION_TASK_POLL_TIME_METRIC_PREFIX
            );

            if (task == null || task.taskToken() == null || task.taskToken().equals("")) {
                log.debug("Polled for decision tasks and there was no work to do.");
                metrics.addCount(NO_DECISION_TASK_TO_EXECUTE_METRIC_NAME, 1.0);
                return null;
            }

            log.debug("Polled for decision task and there was work to do.");
            return ThreadUtils.wrapInExceptionSwallower(() -> executeDecisionTask(task));
        } catch (Throwable e) {
            log.warn("Got an unexpected exception when polling for an decision task.", e);
            throw e;
        }
    }

    private void executeDecisionTask(PollForDecisionTaskResponse task) {
        log.debug("Started work on decision task.");
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder(this.getClass().getSimpleName()
                                                                       + ".executeDecisionTask")) {
            Workflow workflow = workflows.get(task.workflowType().name());
            if (workflow == null) {
                String message = "Decision task received for unrecognized workflow: " + task.workflowType().name();
                log.warn(message);
                throw new UnrecognizedTaskException(message);
            }

            PollForDecisionTaskResponse taskWithFullHistory = fillInEventHistory(metrics, task);

            WorkflowState state = WorkflowState.build(taskWithFullHistory);
            WorkflowStep currentStep = workflowSteps.get(state.getCurrentActivityName());

            RespondDecisionTaskCompletedRequest response = decide(workflow, currentStep,
                                                                  taskWithFullHistory.workflowExecution().workflowId(),
                                                                  state, exponentialBackoffBase,
                                                                  config.getEnablePartitionIdHashing(), metrics, metricsFactory,
                                                                  clock);
            RespondDecisionTaskCompletedRequest responseWithToken
                    = response.toBuilder().taskToken(taskWithFullHistory.taskToken()).build();

            RetryUtils.executeWithInlineBackoff(() -> swf.respondDecisionTaskCompleted(responseWithToken),
                                                20, Duration.ofSeconds(2), metrics,
                                                RESPOND_DECISION_TASK_COMPLETED_METRIC_PREFIX);

            log.debug("Submitted decision for workflow {} id {}.",
                      taskWithFullHistory.workflowType().name(), taskWithFullHistory.workflowExecution().runId());
        } catch (JsonProcessingException e) {
            // wrap in a runtime exception since we can't add a new checked exception to this method's declaration
            throw new RuntimeException(e);
        } finally {
            log.debug("Done working on decision task.");
        }
    }

    private PollForDecisionTaskResponse fillInEventHistory(MetricRecorder metrics,
                                                           PollForDecisionTaskResponse responseWithFirstPage) {
        List<HistoryEvent> events = new ArrayList<>(responseWithFirstPage.events());

        int historyPageCount = 1;

        String nextPageToken = responseWithFirstPage.nextPageToken();
        while (nextPageToken != null && !nextPageToken.equals("")) {
            log.debug("Retrieving another page of history events for task {}", responseWithFirstPage.taskToken());

            PollForDecisionTaskRequest request = PollForDecisionTaskRequest.builder()
                                                                       .domain(domain)
                                                                       .taskList(TaskList.builder().name(taskListName).build())
                                                                       .identity(identity)
                                                                       .reverseOrder(true)
                                                                       .nextPageToken(nextPageToken)
                                                                       .build();

            PollForDecisionTaskResponse nextPaginatedResult
                    = RetryUtils.executeWithInlineBackoff(() -> swf.pollForDecisionTask(request),
                                                          20, Duration.ofSeconds(2), metrics,
                                                          DECISION_TASK_EVENT_HISTORY_LOOKUP_TIME_METRIC_PREFIX);

            if (nextPaginatedResult == null) {
                break;
            }
            if (nextPaginatedResult.events() != null) {
                events.addAll(nextPaginatedResult.events());
            }
            historyPageCount += 1;
            nextPageToken = nextPaginatedResult.nextPageToken();
        }

        metrics.addCount(formatDecisionTaskEventHistoryPageCountMetricName(responseWithFirstPage.workflowType().name()),
                                                                           historyPageCount);

        log.debug("Decision task for workflow {} id {} has {} events.",
                  responseWithFirstPage.workflowType().name(),
                  responseWithFirstPage.workflowExecution().runId(),
                  responseWithFirstPage.events().size()
                  );
        return responseWithFirstPage.toBuilder().nextPageToken(null).events(events).build();

    }

    /**
     * This method makes the actual decision about what to do next for the workflow, given a particular workflow state.
     * - If there is no current step, then the workflow is just starting and the first step should be scheduled.
     * - If the current step succeeded, then we should examine the result code and schedule the next step
     *   as specified by the current step's transitions in the graph. If there isn't one, we fail the decision.
     * - If the current step resulted in a retry, we should schedule the current step again, with an updated retry counter.
     *
     * If we are retrying, we pass the current step's *input* attributes to the next step,
     * plus a RETRY_ATTEMPT attribute (which is either 1, or 1 + the current RETRY_ATTEMPT value).
     * If we are not retrying:
     * - For non-partitioned steps we pass the current step's output attributes as inputs to the next step, except RETRY_ATTEMPT.
     * - For partitioned steps we pass the current step's *input* attributes as inputs to the next step, except RETRY_ATTEMPT,
     *   PARTITION_COUNT, and PARTITION_ID (unless the next step is partitioned, in which case we generate those for the next step).
     *
     * In all cases we ensure that the previous step's ACTIVITY_COMPLETION_MESSAGE and RESULT_CODE
     * attributes are removed from the input.
     *
     * If there is no step to execute next, then we complete the workflow.
     * Otherwise, we schedule the next step or reschedule the current step as appropriate.
     *
     * This method is package-private for testing purposes.
     *
     * @param workflow The workflow to make a decision for
     * @param currentStep The last-executed step of the workflow, if any
     * @param workflowId The user-specified workflow identifier, used for logging purposes.
     * @param state The current state of the workflow
     * @param metrics A MetricRecorder object which will be used to emit framework-level metrics relevant to this workflow.
     * @param metricsFactory A factory used to provide the @PartitionIdGenerator method with a MetricRecorder object if requested.
     * @return A RespondDecisionTaskCompletedRequest containing the list of decisions to be sent to SWF and the execution context.
     *         The caller is responsible for adding the task token to the object.
     */
    static RespondDecisionTaskCompletedRequest decide(Workflow workflow, WorkflowStep currentStep, String workflowId,
                                                      WorkflowState state, double exponentialBackoffBase,
                                                      boolean enablePartitionIdHashing,
                                                      MetricRecorder metrics, MetricRecorderFactory metricsFactory,
                                                      Clock clock)
            throws JsonProcessingException {
        String workflowName = TaskNaming.workflowName(workflow);
        String activityName = (currentStep == null ? null : TaskNaming.activityName(workflowName, currentStep));

        metrics.addProperty(WORKFLOW_ID_METRIC_NAME, workflowId);
        metrics.addProperty(WORKFLOW_RUN_ID_METRIC_NAME, state.getWorkflowRunId());

        List<Decision> decisions = new ArrayList<>();

        String currentStepResultCode = state.getCurrentStepResultCode();

        Map<String, String> nextStepInput = null;

        if (currentStep == null) {
            // the workflow has just started, so the next step's input is the workflow input
            nextStepInput = new HashMap<>(state.getWorkflowInput());
            // Add some workflow attributes to be available to all steps/hooks/etc
            nextStepInput.put(StepAttributes.WORKFLOW_ID, StepAttributes.encode(state.getWorkflowId()));
            nextStepInput.put(StepAttributes.WORKFLOW_EXECUTION_ID, StepAttributes.encode(state.getWorkflowRunId()));
            nextStepInput.put(StepAttributes.WORKFLOW_START_TIME,
                              StepAttributes.encode(state.getWorkflowStartDate()));
        } else {
            // build the next step's input based on the previous step's input and output
            Map<String, PartitionState> currentStepPartitions = state.getLatestPartitionStates(activityName);
            if (currentStepPartitions != null) {
                // Here we just want any arbitrary partition's state, we're just trying to determine the next step's input.
                // We need to make sure we only consider partitions that have state; there is an edge case where
                // we can know about a partition but not have any state for it if SWF failed the first schedule attempt.
                // Note that the .orElse() here can't trigger since we are working against the most recently executed step,
                // for which we always have state.
                // If we didn't have state, then the workflow just started and currentStep would be null, which is handled above.
                PartitionState partition = currentStepPartitions.values().stream()
                                                                .filter(Objects::nonNull).findFirst().orElse(null);

                nextStepInput = new HashMap<>(partition.getAttemptInput());

                boolean currentStepIsPartitioned = PartitionedWorkflowStep.class.isAssignableFrom(currentStep.getClass());
                if (!currentStepIsPartitioned) {
                    // Non-partitioned steps can pass along their output to subsequent steps.
                    // Retry count and result code will be stripped out below.
                    nextStepInput.putAll(partition.getAttemptOutput());
                }

                // Strip out fields that are specific to each attempt, they will be populated below as needed.
                nextStepInput.remove(StepAttributes.PARTITION_ID);
                nextStepInput.remove(StepAttributes.PARTITION_COUNT);
                nextStepInput.remove(StepAttributes.RETRY_ATTEMPT);
                nextStepInput.remove(StepAttributes.RESULT_CODE);
                nextStepInput.remove(StepAttributes.ACTIVITY_COMPLETION_MESSAGE);
            }
        }

        NextStepSelection selection = findNextStep(workflow, currentStep, currentStepResultCode);
        ExecutionContextMetadata executionContext = new ExecutionContextMetadata();
        if (selection.workflowShouldClose() || state.isWorkflowCancelRequested()) {

            // If workflow cancellation was requested, we need to check whether any individual tasks need to be canceled.
            if (state.isWorkflowCancelRequested()) {
                for (PartitionState partition : state.getLatestPartitionStates(activityName).values()) {
                    // For each in-flight partition missing an attempt result (meaning, a worker is actively working it),
                    // request cancellation of the activity task.
                    if (partition != null && partition.getAttemptResult() == null) {
                        decisions.add(buildRequestCancelActivityTaskDecision(partition.getActivityId()));
                    }
                }

                // Also cancel any open retry timers.
                for (String timerId : state.getOpenTimers().keySet()) {
                    decisions.add(buildCancelTimerDecision(timerId));
                }
            }


            Decision decision = handleWorkflowCompletion(workflow, workflowId, state, metrics, clock);
            if (decision != null) {
                decisions.add(decision);
            }

            // if the workflow is ending and this is a periodic workflow, we set the 'next step' field in the execution context
            // to _delayExit to help indicate the reason that the workflow execution hasn't actually closed yet.
            if (workflow.getClass().isAnnotationPresent(Periodic.class)) {
                executionContext.setNextStepName(DELAY_EXIT_TIMER_ID);
            }
        } else if (selection.isNextStepUnknown()) {
            // currentStep can't be null if the next step is unknown, since if the current step is null,
            // we _always_ schedule the first step of the workflow.
            executionContext.populateExecutionContext(currentStep.getClass(), workflow.getGraph());

            decisions.addAll(handleUnknownResultCode(workflow, currentStep, currentStepResultCode, state,
                                                     executionContext.getResultCodeMap().keySet(), metrics));
        } else {
            if (currentStep != null && currentStep != selection.getNextStep()) {
                Duration stepDuration = Duration.between(state.getCurrentStepFirstScheduledTime(),
                                                         state.getCurrentStepCompletionTime());
                metrics.addDuration(formatStepCompletionTimeMetricName(activityName), stepDuration);
                // retryAttempt+1 because if we didn't retry at all, retryAttempt will be 0, and we want total count
                metrics.addCount(formatStepAttemptCountForCompletionMetricName(activityName),
                                 state.getCurrentStepMaxRetryCount() + 1);
            }

            // It's possible we got here after e.g. a CancelWorkflowExecution decision has been made, or if the workflow
            // has otherwise ended. In that case, just bail without making any decisions.
            if (state.isWorkflowExecutionClosed()) {
                log.warn("Workflow {} is already closed, so no new tasks can be scheduled.", workflowId);
            } else {
                decisions.addAll(handleStepScheduling(workflow, workflowId, selection.getNextStep(), state, nextStepInput,
                                                      exponentialBackoffBase, enablePartitionIdHashing,
                                                      metrics, metricsFactory, clock));
                executionContext.populateExecutionContext(selection.getNextStep().getClass(), workflow.getGraph());

            }
        }

        return RespondDecisionTaskCompletedRequest.builder()
                .decisions(decisions)
                .executionContext(executionContext.encode())
                .build();
    }

    private static List<Decision> handleStepScheduling(Workflow workflow, String workflowId, WorkflowStep nextStep,
                                                       WorkflowState state, Map<String, String> nextStepInput,
                                                       double exponentialBackoffBase, boolean enablePartitionIdHashing,
                                                       MetricRecorder fluxMetrics, MetricRecorderFactory metricsFactory,
                                                       Clock clock) throws JsonProcessingException {
        List<Decision> decisions = new LinkedList<>();

        String workflowName = TaskNaming.workflowName(workflow);
        String nextStepName = TaskNaming.stepName(nextStep);
        String nextActivityName = TaskNaming.activityName(workflowName, nextStepName);

        boolean nextStepIsPartitioned = PartitionedWorkflowStep.class.isAssignableFrom(nextStep.getClass());
        PartitionMetadata nextStepPartitionMetadata = state.getPartitionMetadata(nextStepName);

        // We need to generate partition metadata if the next step is partitioned and we don't have any partition metadata.
        if (nextStepIsPartitioned && nextStepPartitionMetadata == null) {
            // If we need to generate partition ids, then we need to return two decisions.
            // First, we need to record a marker containing the partition metadata.
            // Second, like when we cancel a retry timer, we need to force another decision to occur.
            // We do them separately to reduce the likelihood that throttling of activity scheduling affects recording the marker,
            // and (less importantly) because the marker will use up some of the 1MB of data we can send back in the decision
            // response, which may reduce the number of partitions we could schedule.
            PartitionIdGeneratorResult result
                    = WorkflowStepUtil.getPartitionIdsForPartitionedStep((PartitionedWorkflowStep)nextStep,
                                                                         nextStepInput, workflowName,
                                                                         workflowId, metricsFactory);
            PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(result);

            for (String partitionId : metadata.getPartitionIds()) {
                IdentifierValidation.validatePartitionId(partitionId, enablePartitionIdHashing);
            }

            List<String> markerDetailsList = metadata.toMarkerDetailsList();
            for (int i = 0; i < markerDetailsList.size(); i++) {
                String markerDetails = markerDetailsList.get(i);
                RecordMarkerDecisionAttributes markerAttrs = RecordMarkerDecisionAttributes.builder()
                        .markerName(TaskNaming.partitionMetadataMarkerName(nextStepName, i, markerDetailsList.size()))
                        .details(markerDetails)
                        .build();
                Decision marker = Decision.builder().decisionType(DecisionType.RECORD_MARKER)
                        .recordMarkerDecisionAttributes(markerAttrs)
                        .build();
                decisions.add(marker);
            }

            decisions.add(decisionToForceNewDecision());

            // As noted above, we don't want to schedule the partitions at the same time, so we'll return immediately.
            return decisions;
        }

        // Now we need to extract the set of partition IDs.
        Set<String> partitionIds;
        if (nextStepIsPartitioned) {
            partitionIds = nextStepPartitionMetadata.getPartitionIds();
            nextStepInput.putAll(nextStepPartitionMetadata.getEncodedAdditionalAttributes());
        } else {
            partitionIds = Collections.singleton(null);
        }

        for (String partitionId : partitionIds) {
            PartitionState lastAttempt = state.getLatestPartitionStates(nextActivityName).get(partitionId);

            if (lastAttempt != null && lastAttempt.getResultCode() != null) {
                // Non-partitioned steps should not get here.
                if (!nextStepIsPartitioned) {
                    String msg = String.format("We cannot reschedule a non-partitioned step %s.%s that already has a result %s.",
                                               workflowName, nextActivityName, lastAttempt.getResultCode());
                    log.error(msg);
                    throw new BadWorkflowStateException(msg);
                }
                log.info("Workflow {} step {}.{} partition {} already completed ({}), not rescheduling.",
                         workflowId, workflowName, nextActivityName, partitionId, lastAttempt.getResultCode());
                continue;
            }

            boolean retrying = false;
            long attemptNumber = 0L;
            if (lastAttempt != null && lastAttempt.getAttemptResult() == null) {
                // The last attempt was already scheduled (possibly even started) but hasn't finished yet.
                // Just move on to the next partition.
                continue;
            } else if (lastAttempt != null) {
                // This is a retry.
                // We're going to schedule the next attempt, so we need to bump the attempt number.
                attemptNumber = lastAttempt.getRetryAttempt() + 1;
                retrying = true;
            } else { // lastAttempt == null
                // We can get here if we tried to schedule the first attempt of a step
                // but SWF gave us a ScheduleActivityTaskFailed event.
                // In this case we need to try again.
                retrying = true;
            }

            String partitionIdHash = null;
            if (partitionId != null) {
                partitionIdHash = DigestUtils.sha256Hex(partitionId);
            }
            String activityId = TaskNaming.createActivityId(nextStep, attemptNumber, partitionIdHash);

            // We might be transitioning from pre-2.1 workflows or toggling the enableHashedPartitionIds flag, so there may be an
            // existing activity or a signal named using the unhashed partition ID. we'll need to check for that as well.
            String activityIdWithUnhashedPartitionId = TaskNaming.createActivityId(nextStep, attemptNumber, partitionId);

            String activityIdForScheduling = (enablePartitionIdHashing ? activityId : activityIdWithUnhashedPartitionId);

            // For compatibility, we'll check for both activity Ids wherever practical; if we find something using the hashed id,
            // we'll use it, otherwise we'll check for anything with the unhashed id.
            // This way we can process the existing workflow state regardless of whether the flag is being toggled on or off,
            // we only need to consider the flag when scheduling new decisions.

            // For now, the signal matching is done by the specific activity ID in the signal; this means if a signal is addressed
            // to the unhashed activity id, but the hashing flag was enabled when the activity was scheduled, it may not work.
            // https://github.com/danielgmyers/flux-swf-client/issues/104 should address this.
            BaseSignalData signal = state.getSignalsByActivityId().get(activityId);
            if (signal == null) {
                signal = state.getSignalsByActivityId().get(activityIdWithUnhashedPartitionId);
            }

            boolean hasForceResultSignal = false;
            if (signal != null && signal.getSignalType() == SignalType.FORCE_RESULT) {
                hasForceResultSignal = true;
                retrying = false;
            }

            // Now we need to make a decision for this attempt.
            // If it's not the first attempt, we need to check timers for this activityId.
            // - If a timer is open, there's nothing to do yet unless we got a RetryNow or DelayRetry signal,
            //   or the workflow was canceled.
            // - If no timer is open and no timer has fired, add a StartTimer decision.
            // - Otherwise, schedule the next attempt.
            if (attemptNumber > 0) {
                // this is a retry if we get in here.
                if (state.getOpenTimers().containsKey(activityId)
                    || state.getOpenTimers().containsKey(activityIdWithUnhashedPartitionId)) {
                    if (signal != null) {
                        // The retry timer id matches the next attempt's activity id, which matches the signal's activity id.
                        // ScheduleDelayedRetry events need to be ignored if the timer is still open.

                        // first check if the signal is older than the scheduled event for the retry timer, if so ignore it.
                        TimerData openTimer = state.getOpenTimers().get(signal.getActivityId());
                        if (signal.getSignalEventId() > openTimer.getStartTimerEventId()) {
                            decisions.addAll(handleSignal(state, signal, partitionId, nextActivityName, fluxMetrics));
                        }
                        // If the signal was not ForceResult, we don't want to make any more decisions for this partition.
                        // If it was ForceResult, we may or may not need to schedule the next step, we need to let that code decide.
                        if (signal.getSignalType() != SignalType.FORCE_RESULT) {
                            continue;
                        }
                    } else {
                        log.debug("Processed a decision for workflow {} activity {} but there was still an open timer,"
                                  + " no action taken.", workflowId, activityId);
                        // continue to the next partition, we don't want to make any more decisions for this one
                        continue;
                    }
                } else if (signal != null) { // if there isn't an open timer but we have a ScheduleDelayedRetry signal
                    if (signal.getSignalType() == SignalType.SCHEDULE_DELAYED_RETRY) {
                        // first check if the signal is older than the close event for the last retry timer, if so ignore it.
                        if (!state.getClosedTimers().containsKey(signal.getActivityId())
                                || signal.getSignalEventId() > state.getClosedTimers().get(signal.getActivityId())) {
                            decisions.addAll(handleSignal(state, signal, partitionId, nextActivityName, fluxMetrics));
                            // continue to the next partition, we don't want to make any more decisions for this one
                            continue;
                        }
                    } else {
                        log.debug("Processed a decision for workflow {} activity {} but there was no open timer,"
                                  + " no action taken.", workflowId, activityIdForScheduling);
                        // no continue here, we may want to schedule a normal retry timer, or the next step.
                    }
                }

                // check whether we've already started and fired the retry timer. If not, start it.
                if (!state.getClosedTimers().containsKey(activityId) && !state.getOpenTimers().containsKey(activityId)
                    && !state.getClosedTimers().containsKey(activityIdWithUnhashedPartitionId)
                    && !state.getOpenTimers().containsKey(activityIdWithUnhashedPartitionId)) {
                    long delayInSeconds = RetryUtils.calculateRetryBackoffInSeconds(nextStep, attemptNumber,
                                                                                    exponentialBackoffBase);
                    StartTimerDecisionAttributes attrs = buildStartTimerDecisionAttrs(activityIdForScheduling, delayInSeconds,
                                                                                      partitionId);

                    Decision decision = Decision.builder().decisionType(DecisionType.START_TIMER)
                                                          .startTimerDecisionAttributes(attrs)
                                                          .build();

                    log.debug("Workflow {} will have activity {} scheduled after a delay of {} seconds.",
                              workflowId, activityIdForScheduling, delayInSeconds);

                    decisions.add(decision);
                    // continue to the next partition, we don't want to make any more decisions for this one
                    continue;
                }
            } else if (!retrying && state.getCurrentActivityName() != null) {
                // We're about to schedule the first attempt of the next step.
                // We need to check if the previous step ended due to a ForceResult signal; if so,
                // we may need to cancel its retry timer. We don't really know which partition it might have been,
                // so it's easiest to just check for open timers for any partition of the previous step and cancel them.
                Map<String, PartitionState> prevStep = state.getLatestPartitionStates(state.getCurrentActivityName());
                for (Map.Entry<String, PartitionState> prevPartition : prevStep.entrySet()) {
                    PartitionState prevState = prevPartition.getValue();

                    String currentStepName = TaskNaming.stepNameFromActivityName(state.getCurrentActivityName());
                    String prevActivityId = TaskNaming.createActivityId(currentStepName, prevState.getRetryAttempt() + 1,
                                                                        prevPartition.getKey());
                    if (state.getOpenTimers().containsKey(prevActivityId)) {
                        decisions.add(buildCancelTimerDecision(prevActivityId));
                    }
                }
            }

            // if we got a ForceResult signal for this partition attempt, we've probably just cancelled the timer,
            // so we don't want to schedule a retry for it.
            if (hasForceResultSignal) {
                continue;
            }

            // If we get this far, we know we're scheduling a run of nextStep.
            // First let's populate the attempt-specific fields into the next step input map.
            Map<String, String> actualInput = new TreeMap<>(nextStepInput);
            if (attemptNumber > 0L) {
                actualInput.put(StepAttributes.RETRY_ATTEMPT, Long.toString(attemptNumber));
            }
            if (partitionId != null) {
                actualInput.put(StepAttributes.PARTITION_ID, StepAttributes.encode(partitionId));
                actualInput.put(StepAttributes.PARTITION_COUNT, Long.toString(partitionIds.size()));
            }
            Instant firstAttemptDate = state.getStepInitialAttemptTime(nextActivityName);
            if (firstAttemptDate == null) {
                firstAttemptDate = clock.instant();
            }
            actualInput.put(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME, StepAttributes.encode(firstAttemptDate));

            ScheduleActivityTaskDecisionAttributes attrs
                    = buildScheduleActivityTaskDecisionAttrs(workflow, nextStep, actualInput, activityIdForScheduling, partitionId);

            Decision decision = Decision.builder().decisionType(DecisionType.SCHEDULE_ACTIVITY_TASK)
                                                  .scheduleActivityTaskDecisionAttributes(attrs)
                                                  .build();
            decisions.add(decision);

            log.debug("Workflow {} will have activity {} scheduled for execution.", workflowId, activityIdForScheduling);

        }
        return decisions;
    }

    private static List<Decision> handleSignal(WorkflowState state, BaseSignalData signal,
                                               String partitionId, String activityName, MetricRecorder metrics)
            throws JsonProcessingException {
        List<Decision> decisions = new LinkedList<>();
        switch (signal.getSignalType()) {
            case DELAY_RETRY:
                // cancel the timer...
                decisions.add(buildCancelTimerDecision(signal.getActivityId()));

                // ... then schedule it to be recreated with the specified delay.
                SignalExternalWorkflowExecutionDecisionAttributes signalAttrs
                        = buildScheduleRetrySignalAttrs(state, (DelayRetrySignalData)signal);
                Decision scheduleRetry = Decision.builder().decisionType(DecisionType.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION)
                                                           .signalExternalWorkflowExecutionDecisionAttributes(signalAttrs)
                                                           .build();
                decisions.add(scheduleRetry);

                log.debug("Signaling for activity {} to be re-scheduled due to a {} signal.",
                          signal.getActivityId(), signal.getSignalType().getFriendlyName());

                metrics.addCount(formatSignalProcessedForActivityMetricName(activityName, signal.getSignalType()), 1);
                break;
            case SCHEDULE_DELAYED_RETRY:
                if (!state.getOpenTimers().containsKey(signal.getActivityId())) {
                    int delay = ((ScheduleDelayedRetrySignalData) signal).getDelayInSeconds();

                    // then re-create it with the new delay.
                    StartTimerDecisionAttributes timerAttrs = buildStartTimerDecisionAttrs(signal.getActivityId(), delay,
                                                                                           partitionId);
                    Decision startTimer = Decision.builder().decisionType(DecisionType.START_TIMER)
                                                            .startTimerDecisionAttributes(timerAttrs)
                                                            .build();
                    decisions.add(startTimer);
                    log.debug("Setting retry timer for activity {} due to a {} signal, with new delay of {} second{}",
                              signal.getActivityId(), signal.getSignalType().getFriendlyName(), delay,
                              (delay != 1 ? "s." : "."));

                    metrics.addCount(formatSignalProcessedForActivityMetricName(activityName, signal.getSignalType()), 1);
                } else {
                    log.debug("Ignoring signal {} because the timer is still open.", signal.getSignalType().getFriendlyName());
                }
                break;
            case RETRY_NOW:
                decisions.add(buildCancelTimerDecision(signal.getActivityId()));

                // Simply canceling the timer won't cause a new decision task to be scheduled, meaning the step retry would never
                // be scheduled; to solve this, we need to force a new decision task.
                decisions.add(decisionToForceNewDecision());

                log.debug("Immediately retrying activity {} due to {} signal.",
                          signal.getActivityId(), signal.getSignalType().getFriendlyName());

                metrics.addCount(formatSignalProcessedForActivityMetricName(activityName, signal.getSignalType()), 1);
                break;
            case FORCE_RESULT:
                if (state.getOpenTimers().containsKey(signal.getActivityId())) {
                    decisions.add(buildCancelTimerDecision(signal.getActivityId()));
                }
                break;
            default:
                log.warn("Ignoring signal with unknown type: {}", signal.getSignalType());
                break;
        }
        return decisions;
    }

    private static Decision handleWorkflowCompletion(Workflow workflow, String workflowId, WorkflowState state,
                                                     MetricRecorder metrics, Clock clock) {
        String workflowName = TaskNaming.workflowName(workflow);
        String finalActivityName = state.getCurrentActivityName();

        // if we're completing a periodic workflow, we need to set a delayExit timer unless it has already fired
        boolean isPeriodicWorkflow = workflow.getClass().isAnnotationPresent(Periodic.class);
        boolean delayExitTimerHasFired = state.getClosedTimers().containsKey(DELAY_EXIT_TIMER_ID);
        if (isPeriodicWorkflow && !delayExitTimerHasFired) {

            // if the timer is still open we should do nothing here. Returning a null decision will do the trick.
            if (state.getOpenTimers().containsKey(DELAY_EXIT_TIMER_ID)) {
                log.debug("Processed a decision for {} but there was still an open timer, no action taken.", workflowId);
                return null;
            }

            Periodic periodicConfig = workflow.getClass().getAnnotation(Periodic.class);

            long runIntervalSeconds = periodicConfig.intervalUnits().toSeconds(periodicConfig.runInterval());
            Instant expectedWorkflowEnd = state.getWorkflowStartDate().plusSeconds(runIntervalSeconds);

            // we are probably going to delay exit, but for the purposes of completion time we want to exclude the delay.
            Duration executionDuration = Duration.between(state.getWorkflowStartDate(), state.getCurrentStepCompletionTime());
            metrics.addDuration(formatWorkflowCompletionTimeMetricName(workflowName), executionDuration);

            // we also want to emit the execution time for the last step, again excluding the delay.

            // for periodic workflows, we don't want to emit the completion time metric again when the workflow ends,
            // because it was already emitted once when the delayExit timer was scheduled.
            if (state.getCurrentActivityName() != null) {
                Duration stepCompletionTime = Duration.between(state.getCurrentStepFirstScheduledTime(),
                                                               state.getCurrentStepCompletionTime());
                metrics.addDuration(formatStepCompletionTimeMetricName(state.getCurrentActivityName()), stepCompletionTime);
                // +1 because we want to count the original attempt
                metrics.addCount(formatStepAttemptCountForCompletionMetricName(finalActivityName),
                                 state.getCurrentStepMaxRetryCount() + 1);
            }

            // Figure out how much time is left between now and the expected workflow end date
            Duration duration = Duration.between(clock.instant(), expectedWorkflowEnd);

            // always delay at least one second just so there's always a timer when we handle these workflows
            long delayInSeconds = Math.max(1, duration.getSeconds());

            StartTimerDecisionAttributes attrs = buildStartTimerDecisionAttrs(DELAY_EXIT_TIMER_ID, delayInSeconds, null);

            Decision decision = Decision.builder().decisionType(DecisionType.START_TIMER)
                                                  .startTimerDecisionAttributes(attrs)
                                                  .build();

            log.debug("Periodic Workflow {} will close after a delayed exit in {} seconds.", workflowId, delayInSeconds);
            return decision;
        }

        // At this point we know we don't need to set a delayExit timer
        Decision decision;

        // The workflow has ended, but we need to determine whether it succeeded or failed.
        final String resultCode = state.getCurrentStepResultCode();
        if (isPeriodicWorkflow) {
            // if this was a periodic workflow, we'll return a ContinueAsNew decision so that it restarts immediately.
            ContinueAsNewWorkflowExecutionDecisionAttributes attrs
                    = ContinueAsNewWorkflowExecutionDecisionAttributes.builder()
                        .childPolicy(ChildPolicy.TERMINATE)
                        // map values are already serialized in this workflow's original input
                        .input(StepAttributes.encode(state.getWorkflowInput()))
                        .taskStartToCloseTimeout(FluxCapacitorImpl.DEFAULT_DECISION_TASK_TIMEOUT)
                        .executionStartToCloseTimeout(Long.toString(workflow.maxStartToCloseDuration().getSeconds()))
                        .taskList(TaskList.builder().name(workflow.taskList()).build())
                        .build();

            decision = Decision.builder().decisionType(DecisionType.CONTINUE_AS_NEW_WORKFLOW_EXECUTION)
                                         .continueAsNewWorkflowExecutionDecisionAttributes(attrs)
                                         .build();
        } else if (state.isWorkflowCancelRequested()) {
            // If a workflow cancellation was requested, cancel the workflow
            CancelWorkflowExecutionDecisionAttributes attrs = CancelWorkflowExecutionDecisionAttributes.builder().build();
            decision = Decision.builder().decisionType(DecisionType.CANCEL_WORKFLOW_EXECUTION)
                                         .cancelWorkflowExecutionDecisionAttributes(attrs)
                                         .build();
        } else if (StepResult.FAIL_RESULT_CODE.equals(resultCode)) {
            // terminate the workflow as failed if the last step had a failed result.
            String reason = finalActivityName + " failed after " + state.getCurrentStepMaxRetryCount() + " attempts.";
            String details = state.getCurrentStepLastActivityCompletionMessage();
            if (details == null) {
                details = "No details were provided by the last activity: " + finalActivityName;
            }

            FailWorkflowExecutionDecisionAttributes attrs = FailWorkflowExecutionDecisionAttributes.builder()
                    .reason(reason).details(details).build();

            decision = Decision.builder().decisionType(DecisionType.FAIL_WORKFLOW_EXECUTION)
                                         .failWorkflowExecutionDecisionAttributes(attrs)
                                         .build();
        } else {
            CompleteWorkflowExecutionDecisionAttributes attrs = CompleteWorkflowExecutionDecisionAttributes.builder().build();
            decision = Decision.builder().decisionType(DecisionType.COMPLETE_WORKFLOW_EXECUTION)
                                         .completeWorkflowExecutionDecisionAttributes(attrs)
                                         .build();
        }

        // Periodic workflows have these metrics emitted *before* the delayExit fires, don't do it again in that case.
        if (!isPeriodicWorkflow) {
            Instant completionDate = state.getCurrentStepCompletionTime();
            if (state.isWorkflowCancelRequested()) {
                completionDate = state.getWorkflowCancelRequestDate();
            }
            Duration executionDuration = Duration.between(state.getWorkflowStartDate(), completionDate);
            metrics.addDuration(formatWorkflowCompletionTimeMetricName(workflowName), executionDuration);

            if (state.getCurrentActivityName() != null) {
                completionDate = state.getCurrentStepCompletionTime();
                if (completionDate == null && state.isWorkflowCancelRequested()) {
                    completionDate = state.getWorkflowCancelRequestDate();
                }
                Duration stepCompletionTime = Duration.between(state.getCurrentStepFirstScheduledTime(),
                                                               completionDate);
                metrics.addDuration(formatStepCompletionTimeMetricName(state.getCurrentActivityName()), stepCompletionTime);
                // +1 because we want to count the original attempt
                metrics.addCount(formatStepAttemptCountForCompletionMetricName(finalActivityName),
                                 state.getCurrentStepMaxRetryCount() + 1);
            }
        }

        log.debug("Workflow {} will be closed as successful.", workflowId);
        return decision;
    }

    private static List<Decision> handleUnknownResultCode(Workflow workflow, WorkflowStep currentStep, String actualResultCode,
                                                          WorkflowState state, Set<String> validResultCodes,
                                                          MetricRecorder metrics) {
        List<Decision> decisions = new ArrayList<>();

        RecordMarkerDecisionAttributes markerAttrs = RecordMarkerDecisionAttributes.builder()
                .markerName("UnknownResultCode")
                .details("Unrecognized result code '" + actualResultCode + "' for workflow step "
                         + currentStep.getClass().getSimpleName() + ". Valid result codes: "
                         + String.join(", ", validResultCodes))
                .build();
        decisions.add(Decision.builder().decisionType(DecisionType.RECORD_MARKER)
                              .recordMarkerDecisionAttributes(markerAttrs).build());

        // We'll emit three metrics. First, a top-level metric, useful for alarming across all workflows.
        // Then, a workflow-level and step-level metric, useful for deep-diving.
        metrics.addCount(UNKNOWN_RESULT_CODE_METRIC_BASE, 1);
        metrics.addCount(formatUnknownResultCodeWorkflowMetricName(TaskNaming.workflowName(workflow)), 1);
        metrics.addCount(formatUnknownResultCodeWorkflowStepMetricName(TaskNaming.activityName(workflow, currentStep)), 1);

        // the timer might already be open from a previous attempt to make this decision
        if (!state.getOpenTimers().containsKey(UNKNOWN_RESULT_RETRY_TIMER_ID)) {
            StartTimerDecisionAttributes timerAttrs
                    = buildStartTimerDecisionAttrs(UNKNOWN_RESULT_RETRY_TIMER_ID,
                                                   UNKNOWN_RESULT_RETRY_TIMER_DELAY.getSeconds(), null);
            decisions.add(Decision.builder().decisionType(DecisionType.START_TIMER)
                                  .startTimerDecisionAttributes(timerAttrs).build());
        }

        return decisions;
    }

    // package-private for testing
    // returns the next step of the workflow that should be executed.
    // if the result code is null or blank, assumes the next step should be to retry the current step.
    static NextStepSelection findNextStep(Workflow workflow, WorkflowStep currentStep, String resultCode) {
        WorkflowGraph graph = workflow.getGraph();

        // If there isn't a current step, then the workflow has just started, and we pick the first step.
        if (currentStep == null) {
            return NextStepSelection.scheduleNextStep(graph.getFirstStep());
        } else if (resultCode == null || resultCode.isEmpty()) {
            return NextStepSelection.scheduleNextStep(currentStep);
        }

        WorkflowGraphNode currentNode = graph.getNodes().get(currentStep.getClass());
        String effectiveResultCode = resultCode;
        if (currentNode.getNextStepsByResultCode().containsKey(StepResult.ALWAYS_RESULT_CODE)) {
            effectiveResultCode = StepResult.ALWAYS_RESULT_CODE;
        } else if (!currentNode.getNextStepsByResultCode().containsKey(effectiveResultCode)) {
            // this can happen during deployments that add new result codes, on the workers with the old code
            return NextStepSelection.unknownResultCode();
        }

        WorkflowGraphNode selectedStep = currentNode.getNextStepsByResultCode().get(effectiveResultCode);

        // if the transition for this result code is null, we should close the workflow.
        if (selectedStep == null) {
            return NextStepSelection.closeWorkflow();
        }

        return NextStepSelection.scheduleNextStep(selectedStep.getStep());
    }

    // package-private for use in tests
    static ScheduleActivityTaskDecisionAttributes
            buildScheduleActivityTaskDecisionAttrs(Workflow workflow, WorkflowStep nextStep,
                                                   Map<String, String> nextStepInput, String activityId, String partitionId) {
        String activityName = TaskNaming.activityName(workflow, nextStep);
        return ScheduleActivityTaskDecisionAttributes.builder()
                .taskList(TaskList.builder().name(workflow.taskList()).build())
                .input(StepAttributes.encode(nextStepInput))
                .heartbeatTimeout(Long.toString(nextStep.activityTaskHeartbeatTimeout().getSeconds()))
                .scheduleToStartTimeout("NONE")
                .scheduleToCloseTimeout("NONE")
                .startToCloseTimeout("NONE")
                // We'll save the partition id in the control field for convenience in debugging and testing,
                // and to reference when rebuilding partition state to avoid having to inspect the step's input attributes.
                .control(partitionId)
                .activityType(ActivityType.builder().name(activityName).version(FluxCapacitorImpl.WORKFLOW_VERSION).build())
                .activityId(activityId)
                .build();
    }

    // package-private for use in tests
    static StartTimerDecisionAttributes buildStartTimerDecisionAttrs(String timerId, long delayInSeconds, String partitionId) {
        return StartTimerDecisionAttributes.builder()
                .timerId(timerId)
                .startToFireTimeout(Long.toString(delayInSeconds))
                // We'll save the partition id in the control field for convenience in debugging and testing.
                .control(partitionId)
                .build();
    }

    private static Decision buildRequestCancelActivityTaskDecision(String activityId) {
        RequestCancelActivityTaskDecisionAttributes attrs
                = RequestCancelActivityTaskDecisionAttributes.builder().activityId(activityId).build();

        return Decision.builder().decisionType(DecisionType.REQUEST_CANCEL_ACTIVITY_TASK)
                                 .requestCancelActivityTaskDecisionAttributes(attrs)
                                 .build();
    }

    private static Decision buildCancelTimerDecision(String timerId) {
        CancelTimerDecisionAttributes attrs = CancelTimerDecisionAttributes.builder().timerId(timerId).build();
        return Decision.builder().decisionType(DecisionType.CANCEL_TIMER)
                                 .cancelTimerDecisionAttributes(attrs)
                                 .build();
    }

    /**
     * There are two relatively easy ways to force a new decision task to occur:
     * 1) Return a SignalExternalWorkflowExecutionDecision telling this workflow to signal itself.
     * 2) Set a zero-second timer.
     *
     * The first option causes three events to be added to the workflow, with a new decision task sometimes taking place
     * before the third event (an ExternalWorkflowSignaled notification) is even added to the event history,
     * due to the asynchronous signaling machinery in SWF.
     *
     * The second option only results in two events, StartTimer and TimerFired, and the second happens immediately.
     *
     * Previously Flux used the first option but the second option is lighter weight.
     *
     * Visible for use in tests.
     */
    static Decision decisionToForceNewDecision() {
        return Decision.builder().decisionType(DecisionType.START_TIMER)
                .startTimerDecisionAttributes(buildStartTimerDecisionAttrs(FORCE_NEW_DECISION_TIMER_ID, 0, null))
                .build();
    }

    // package-private for use in tests
    static SignalExternalWorkflowExecutionDecisionAttributes buildScheduleRetrySignalAttrs(WorkflowState state,
                                                                                           DelayRetrySignalData originalSignal)
            throws JsonProcessingException {
        return SignalExternalWorkflowExecutionDecisionAttributes.builder()
                .signalName(SignalType.SCHEDULE_DELAYED_RETRY.getFriendlyName())
                .input(new ScheduleDelayedRetrySignalData(originalSignal).toJson())
                .workflowId(state.getWorkflowId())
                .runId(state.getWorkflowRunId())
                .build();
    }

    // package-private for test visibility
    static String formatSignalProcessedForActivityMetricName(String activityName, SignalType signalType) {
        return String.format("Flux.SignalProcessed.%s.%s", activityName, signalType.getFriendlyName());
    }

    // package-private for test visibility
    static String formatDecisionTaskEventHistoryPageCountMetricName(String workflowName) {
        return String.format("Flux.DecisionTaskEventHistoryPageCount.%s", workflowName);
    }

    // package-private for test visibility
    static String formatStepCompletionTimeMetricName(String activityName) {
        return String.format("Flux.StepCompletionTime.%s", activityName);
    }

    // package-private for test visibility
    static String formatStepAttemptCountForCompletionMetricName(String activityName) {
        return String.format("Flux.StepAttemptCountForCompletion.%s", activityName);
    }

    // package-private for test visibility
    static String formatWorkflowCompletionTimeMetricName(String workflowName) {
        return String.format("Flux.WorkflowCompletionTime.%s", workflowName);
    }

    // package-private for test visibility
    static String formatUnknownResultCodeWorkflowMetricName(String workflowName) {
        return String.format("%s.%s", UNKNOWN_RESULT_CODE_METRIC_BASE, workflowName);
    }

    // package-private for test visibility
    static String formatUnknownResultCodeWorkflowStepMetricName(String activityName) {
        return String.format("%s.%s", UNKNOWN_RESULT_CODE_METRIC_BASE, activityName);
    }
}
