package software.amazon.aws.clients.swf.flux.tests;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.wf.Periodic;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

/**
 * Tests that validate Flux's behavior for @Periodic workflows.
 */
public class PeriodicWorkflowTests extends WorkflowTestBase {
    private static final Logger log = LoggerFactory.getLogger(PeriodicWorkflowTests.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new PeriodicHello());
    }

    /**
     * Tests that a periodic workflow is executed the expected number of times in a specific interval.
     */
    @Test
    public void testPeriodicWorkflow() throws InterruptedException {
        log.info("Sleeping for 10 seconds. After this, the execution count should be 1.");
        Thread.sleep(10000);
        Assert.assertEquals(1, Step.getExecutionCount());
        log.info("Waiting for 20 seconds, should do 1 more execution in that time.");
        Thread.sleep(20000);
        Assert.assertEquals(2, Step.getExecutionCount());
        log.info("Waiting for 40 seconds, should do 2 more executions in that time.");
        Thread.sleep(40000);
        Assert.assertEquals(4, Step.getExecutionCount());
    }

    /**
     * Periodic workflow with one step.
     */
    @Periodic(runInterval = 20, intervalUnits = TimeUnit.SECONDS)
    public static final class PeriodicHello implements Workflow {
        private final WorkflowGraph graph;

        PeriodicHello() {
            WorkflowStep step = new Step();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(step, Collections.emptyMap());
            builder.alwaysClose(step);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }
    }

    /**
     * Simple step that records which workflow IDs the step is executed for.
     */
    public static final class Step implements WorkflowStep {
        private static final AtomicInteger executionCount = new AtomicInteger(0);

        @StepApply
        public void doThing() {
            executionCount.incrementAndGet();
        }

        static int getExecutionCount() {
            return executionCount.get();
        }
    }
}
