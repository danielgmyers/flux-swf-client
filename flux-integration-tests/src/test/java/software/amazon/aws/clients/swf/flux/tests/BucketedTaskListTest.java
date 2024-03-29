package software.amazon.aws.clients.swf.flux.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.FluxCapacitorConfig;
import software.amazon.aws.clients.swf.flux.TaskListConfig;
import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Validates bucketed task list functionality.
 */
public class BucketedTaskListTest extends WorkflowTestBase {

    private static final Logger log = LoggerFactory.getLogger(BucketedTaskListTest.class);

    private static final String TASK_LIST_NAME = "three-iron-ingots";

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new BucketedHelloWorld());
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
            Assert.assertEquals(Collections.singleton(TASK_LIST_NAME), new HashSet<>(info.tagList()));

            GetWorkflowExecutionHistoryResponse response = getWorkflowExecutionHistory(uuid, info.execution().runId());
            Assert.assertTrue(response.hasEvents());
            String taskList = response.events().get(0).workflowExecutionStartedEventAttributes().taskList().name();
            assignedTaskLists.add(taskList);
            log.info("Workflow " + uuid + " ran on task list " + taskList);

            Assert.assertTrue(StepOne.didExecute(uuid));
        }

        // It's technically possible for this to fail, if the random bucket number selection logic chooses bucket 1
        // for all ten executions. This is probably fine to live with.
        Assert.assertTrue(assignedTaskLists.size() > 1);

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
