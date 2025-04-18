package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.danielgmyers.flux.signals.SignalType;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Tests that validate Flux's signal support.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class SignalTests extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(SignalTests.class);

    private static final String RESULT_CODE_THAT_CLOSES_WORKFLOW = "IMustBeForcedToClose";

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Arrays.asList(new RequiresForcedResult(), new DoesNotHandleCustomResultCode(),
                             new RetriesAFewTimes(), new RetriesOnceWithLongRetryTime());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Test
    public void testRetriesExpectedNumberOfTimesBeforeForceResultSignal() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(RequiresForcedResult.class, uuid, Collections.emptyMap());
        log.info("Sleeping for 6 seconds, there should be 1 attempt after this...");
        Thread.sleep(6000);
        Assertions.assertEquals(1, AlwaysRetries.getAttemptCount(uuid));

        log.info("Sleeping for 20 seconds, there should be 1 more attempt after this...");
        Thread.sleep(20000);
        Assertions.assertEquals(2, AlwaysRetries.getAttemptCount(uuid));

        // we need to know the activity name and the next attempt number (which is zero-based).
        signalWorkflowExecution(uuid, SignalType.FORCE_RESULT.getFriendlyName(),
                                String.format("{\"activityId\": \"%s\", \"resultCode\": \"%s\" }",
                                              String.format("%s_%s", AlwaysRetries.class.getSimpleName(), 2),
                                              RESULT_CODE_THAT_CLOSES_WORKFLOW));
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        // since we forced the workflow to close, it should have closed without running the step again.
        Assertions.assertEquals(2, AlwaysRetries.getAttemptCount(uuid));
    }

    @Test
    public void testWorkflowWithUnknownResultCodeSucceedsAfterForceResult() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(DoesNotHandleCustomResultCode.class, uuid, Collections.emptyMap());
        log.info("Sleeping for 6 seconds to give the step time to run...");
        Thread.sleep(6000);
        Assertions.assertEquals(1, SucceedWithCustomResultCode.getAttemptCount(uuid));

        // At this point, the workflow would have succeeded if it handled the custom result code.
        // Since it didn't, we need to override the result of the first step attempt, i.e. attempt 0.

        // We need to know the activity name and the next attempt number (which is zero-based).
        // Note we use attempt 1 in the signal name even though there won't be a second attempt,
        // because Flux only looks at (last attempt + 1) for applicable signals.
        signalWorkflowExecution(uuid, SignalType.FORCE_RESULT.getFriendlyName(),
                                String.format("{\"activityId\": \"%s\", \"resultCode\": \"%s\" }",
                                              String.format("%s_%s", SucceedWithCustomResultCode.class.getSimpleName(), 1),
                                              StepResult.SUCCEED_RESULT_CODE));
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(30));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        // since we forced the workflow to close, it should have closed without running the step again.
        Assertions.assertEquals(1, SucceedWithCustomResultCode.getAttemptCount(uuid));
    }

    @Test
    public void testRetryNowCausesEarlyRetry() throws InterruptedException {
        log.info("Running workflow with long retry time...");
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(RetriesOnceWithLongRetryTime.class, uuid, Collections.emptyMap());
        log.info("Sleeping for 6 seconds, there should be 1 attempt after this...");
        Thread.sleep(6000);
        Assertions.assertEquals(1, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));

        log.info("Sleeping for 40 seconds, there should be 1 more attempt after this...");
        Thread.sleep(40000);
        Assertions.assertEquals(2, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));

        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(10));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        // It should have closed without running the step again.
        Assertions.assertEquals(2, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));

        log.info("Running workflow with long retry time again...");

        uuid = UUID.randomUUID().toString();
        executeWorkflow(RetriesOnceWithLongRetryTime.class, uuid, Collections.emptyMap());
        log.info("Sleeping for 6 seconds, there should be 1 more attempt after this...");
        Thread.sleep(6000);
        Assertions.assertEquals(1, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));

        // we need to know the activity name and the next attempt number (which is zero-based).
        signalWorkflowExecution(uuid, SignalType.RETRY_NOW.getFriendlyName(),
                                String.format("{\"activityId\": \"%s\"}",
                                              String.format("%s_%s", SucceedsOnRetryAttemptOne.class.getSimpleName(), 1)));

        log.info("Sleeping for 10 seconds, there should be 1 more attempt after this...");
        Thread.sleep(10000);
        Assertions.assertEquals(2, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));

        info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(10));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        // It should have closed without running the step again.
        Assertions.assertEquals(2, SucceedsOnRetryAttemptOne.getAttemptCount(uuid));
    }

    @Test
    public void testDelayRetryActuallyDelaysRetry() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(RetriesAFewTimes.class, uuid, Collections.emptyMap());
        log.info("Sleeping for 6 seconds, there should be 1 attempt after this...");
        Thread.sleep(6000);
        Assertions.assertEquals(1, SucceedsOnRetryAttemptTwo.getAttemptCount(uuid));

        // we need to know the activity name and the next attempt number (which is zero-based).
        signalWorkflowExecution(uuid, SignalType.DELAY_RETRY.getFriendlyName(),
                                String.format("{\"activityId\": \"%s\", \"delayInSeconds\": 40 }",
                                              String.format("%s_%s", SucceedsOnRetryAttemptTwo.class.getSimpleName(), 1)));

        // Note that delay-retry signals can take ~5 seconds to fully process since they involve multiple decision tasks;
        // the initial retry timer is cancelled as soon as the first signal is received,
        // but a second signal is sent to restart the retry timer, and that may be several seconds later.

        log.info("Sleeping for 16 seconds, there should not have been a second attempt after this...");
        Thread.sleep(16000);
        Assertions.assertEquals(1, SucceedsOnRetryAttemptTwo.getAttemptCount(uuid));

        log.info("Sleeping for another 30 seconds, there should have been a second attempt after this...");
        Thread.sleep(30000);
        Assertions.assertEquals(2, SucceedsOnRetryAttemptTwo.getAttemptCount(uuid));

        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(120));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        // Since the workflow only ends after its second retry attempt (third step execution), there should be three attempts now.
        Assertions.assertEquals(3, SucceedsOnRetryAttemptTwo.getAttemptCount(uuid));
    }

    /**
     * Workflow with one step that retries a few times before it succeeds.
     */
    public static final class RetriesOnceWithLongRetryTime implements Workflow {
        private final WorkflowGraph graph;

        RetriesOnceWithLongRetryTime() {
            WorkflowStep stepOne = new SucceedsOnRetryAttemptOne();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.alwaysClose(stepOne);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that retries once with a 40-second retry time.
     */
    public static final class SucceedsOnRetryAttemptOne implements WorkflowStep {
        private static final Map<String, Long> attemptCounts = Collections.synchronizedMap(new HashMap<>());

        /**
         * This step forces a bunch of the retry timing parameters so the test doesn't risk getting the timing wrong
         * due to jitter or backoff.
         */
        @StepApply(initialRetryDelaySeconds = 40, retriesBeforeBackoff = 6, jitterPercent = 0, maxRetryDelaySeconds = 40)
        public StepResult doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                  @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            attemptCounts.put(workflowId, (retryAttempt == null ? 1 : (retryAttempt + 1)));
            if (Long.valueOf(1).equals(retryAttempt)) {
                return StepResult.success("Succeeded on retry attempt 1");
            } else {
                return StepResult.retry("Haven't reached retry attempt 1 yet, retrying.");
            }
        }

        static int getAttemptCount(String workflowId) {
            return attemptCounts.get(workflowId).intValue();
        }
    }

    /**
     * Workflow with one step that retries a few times before it succeeds.
     */
    public static final class RetriesAFewTimes implements Workflow {
        private final WorkflowGraph graph;

        RetriesAFewTimes() {
            WorkflowStep stepOne = new SucceedsOnRetryAttemptTwo();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.alwaysClose(stepOne);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that retries twice with a 20-second retry time.
     */
    public static final class SucceedsOnRetryAttemptTwo implements WorkflowStep {
        private static final Map<String, Long> attemptCounts = Collections.synchronizedMap(new HashMap<>());

        /**
         * This step forces a bunch of the retry timing parameters so the test doesn't risk getting the timing wrong
         * due to jitter or backoff.
         */
        @StepApply(initialRetryDelaySeconds = 20, retriesBeforeBackoff = 6, jitterPercent = 0, maxRetryDelaySeconds = 20)
        public StepResult doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                  @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            attemptCounts.put(workflowId, (retryAttempt == null ? 1 : (retryAttempt + 1)));
            if (Long.valueOf(2).equals(retryAttempt)) {
                return StepResult.success("Succeeded on retry attempt 2");
            } else {
                return StepResult.retry("Haven't reached retry attempt 2 yet, retrying.");
            }
        }

        static int getAttemptCount(String workflowId) {
            return attemptCounts.get(workflowId).intValue();
        }
    }

    /**
     * Workflow with one step that always retries. It can only succeed via ForceResultSignal
     */
    public static final class RequiresForcedResult implements Workflow {
        private final WorkflowGraph graph;

        RequiresForcedResult() {
            WorkflowStep stepOne = new AlwaysRetries();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.closeOnCustom(stepOne, RESULT_CODE_THAT_CLOSES_WORKFLOW);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that always retries.
     */
    public static final class AlwaysRetries implements WorkflowStep {
        private static final Map<String, Long> attemptCounts = Collections.synchronizedMap(new HashMap<>());

        /**
         * This step forces a bunch of the retry timing parameters so the test doesn't risk getting the timing wrong
         * due to jitter or backoff.
         */
        @StepApply(initialRetryDelaySeconds = 20, retriesBeforeBackoff = 6, jitterPercent = 0, maxRetryDelaySeconds = 20)
        public StepResult doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                  @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            attemptCounts.put(workflowId, (retryAttempt == null ? 1 : (retryAttempt + 1)));
            return StepResult.retry("Always retrying!");
        }

        static int getAttemptCount(String workflowId) {
            return attemptCounts.get(workflowId).intValue();
        }
    }

    /**
     * Workflow with one step that returns a result code that has no transition.
     * It can only succeed via ForceResultSignal.
     */
    public static final class DoesNotHandleCustomResultCode implements Workflow {
        private final WorkflowGraph graph;

        DoesNotHandleCustomResultCode() {
            WorkflowStep stepOne = new SucceedWithCustomResultCode();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.closeOnSuccess(stepOne);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that always succeeds, but uses a custom result code.
     */
    public static final class SucceedWithCustomResultCode implements WorkflowStep {
        private static final Map<String, Long> attemptCounts = Collections.synchronizedMap(new HashMap<>());

        @StepApply
        public StepResult doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                                  @Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            attemptCounts.put(workflowId, (retryAttempt == null ? 1 : (retryAttempt + 1)));
            return StepResult.complete("unknown-result-code", "The graph shouldn't know what to do with this code");
        }

        static int getAttemptCount(String workflowId) {
            return attemptCounts.get(workflowId).intValue();
        }
    }
}
