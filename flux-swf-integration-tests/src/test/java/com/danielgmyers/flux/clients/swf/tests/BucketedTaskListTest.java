package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.TaskListConfig;
import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
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

import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Validates bucketed task list functionality.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class BucketedTaskListTest extends WorkflowTestBase {

    private static final Logger log = LoggerFactory.getLogger(BucketedTaskListTest.class);

    private static final String TASK_LIST_NAME = "three-iron-ingots";

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new BucketedHelloWorld());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Override
    protected void updateFluxCapacitorConfig(FluxCapacitorConfig config) {
        TaskListConfig taskListConfig = new TaskListConfig();
        taskListConfig.setBucketCount(10);
        config.putTaskListConfig(TASK_LIST_NAME, taskListConfig);
    }

    /**
     * Tests that a single-step workflow is executed the expected number of times by bucketed worker threads.
     */
    @Test
    public void testBucketedTaskList() throws InterruptedException {
        Set<String> assignedTaskLists = new TreeSet<>(); // tree set so that they're in sorted order in the log message
        for (int i = 0; i < 10; i++) {
            String uuid = UUID.randomUUID().toString();

            executeWorkflow(BucketedHelloWorld.class, uuid, Collections.emptyMap());
            WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(15));

            // the execution tags should have the non-bucketed task list name
            Assertions.assertEquals(Collections.singleton(TASK_LIST_NAME), new HashSet<>(info.tagList()));

            GetWorkflowExecutionHistoryResponse response = getWorkflowExecutionHistory(uuid, info.execution().runId());
            Assertions.assertTrue(response.hasEvents());
            String taskList = response.events().get(0).workflowExecutionStartedEventAttributes().taskList().name();
            assignedTaskLists.add(taskList);
            log.info("Workflow " + uuid + " ran on task list " + taskList);

            Assertions.assertTrue(StepOne.didExecute(uuid));
        }

        // It's technically possible for this to fail, if the random bucket number selection logic chooses bucket 1
        // for all ten executions. This is probably fine to live with.
        Assertions.assertTrue(assignedTaskLists.size() > 1);

        log.info("Ran workflows on these " + assignedTaskLists.size() + " task lists: " + assignedTaskLists);
    }

    /**
     * Basic workflow with one step.
     */
    public static final class BucketedHelloWorld implements Workflow {
        private final WorkflowGraph graph;

        BucketedHelloWorld() {
            WorkflowStep stepOne = new StepOne();
            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne, Collections.emptyMap());
            builder.alwaysClose(stepOne);
            graph = builder.build();
        }

        @Override
        public WorkflowGraph getGraph() {
            return graph;
        }

        @Override
        public String taskList() {
            return TASK_LIST_NAME;
        }
    }

    /**
     * Simple step that records which workflow IDs the step is executed for.
     */
    public static final class StepOne implements WorkflowStep {
        private static final Set<String> executedWorkflowIds = Collections.synchronizedSet(new HashSet<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            executedWorkflowIds.add(workflowId);
        }

        static boolean didExecute(String workflowId) {
            return executedWorkflowIds.contains(workflowId);
        }
    }
}
