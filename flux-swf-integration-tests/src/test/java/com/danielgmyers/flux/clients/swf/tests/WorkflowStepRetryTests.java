package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

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
 * Validates that the two ways a step can retry both result in retries and that workflows can still complete afterward.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class WorkflowStepRetryTests extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(WorkflowStepRetryTests.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Arrays.asList(new WorkflowWithRetryingStep(),
                             new WorkflowWithExceptionThrowingStep());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    /**
     * Tests that a single-step workflow is executed the expected number of times by worker threads.
     */
    @Test
    public void testRetryWithStepResult() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        executeWorkflow(WorkflowWithRetryingStep.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));
    }

    /**
     * Tests that a single-step workflow is executed the expected number of times by worker threads.
     */
    @Test
    public void testRetryWithException() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        executeWorkflow(WorkflowWithExceptionThrowingStep.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));
    }

    /**
     * Workflow with a single retrying step.
     */
    public static final class WorkflowWithRetryingStep implements Workflow {
        private final WorkflowGraph graph;

        WorkflowWithRetryingStep() {
            WorkflowStep stepOne = new RetriesTwiceBeforeSucceeding();
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
     * Simple step that retries twice before succeeding.
     */
    public static final class RetriesTwiceBeforeSucceeding implements WorkflowStep {
        /**
         * Does the thing.
         */
        @StepApply
        public StepResult doThing(@Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            if (retryAttempt == null || retryAttempt < 2) {
                return StepResult.retry("Haven't retried twice yet");
            }
            return StepResult.success();
        }
    }

    /**
     * Workflow with a single retrying-via-exception step.
     */
    public static final class WorkflowWithExceptionThrowingStep implements Workflow {
        private final WorkflowGraph graph;

        WorkflowWithExceptionThrowingStep() {
            WorkflowStep stepOne = new ThrowsTwiceBeforeSucceeding();
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
     * Simple step that retries twice (by throwing an exception) before succeeding.
     */
    public static final class ThrowsTwiceBeforeSucceeding implements WorkflowStep {
        /**
         * Does the thing.
         */
        @StepApply
        public StepResult doThing(@Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            if (retryAttempt == null || retryAttempt < 2) {
                throw new RuntimeException("Haven't retried twice yet");
            }
            return StepResult.success();
        }
    }
}
