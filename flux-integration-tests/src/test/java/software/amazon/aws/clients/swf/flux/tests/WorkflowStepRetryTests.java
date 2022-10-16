package software.amazon.aws.clients.swf.flux.tests;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Validates that the two ways a step can retry both result in retries and that workflows can still complete afterward.
 */
public class WorkflowStepRetryTests extends WorkflowTestBase {
    @Override
    List<Workflow> getWorkflowsForTest() {
        return Arrays.asList(new WorkflowWithRetryingStep(),
                             new WorkflowWithExceptionThrowingStep());
    }

    /**
     * Tests that a single-step workflow is executed the expected number of times by worker threads.
     */
    @Test
    public void testRetryWithStepResult() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();

        executeWorkflow(WorkflowWithRetryingStep.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
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

        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
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
