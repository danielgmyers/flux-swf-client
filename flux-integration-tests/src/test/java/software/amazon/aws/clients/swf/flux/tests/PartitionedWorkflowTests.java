package software.amazon.aws.clients.swf.flux.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.step.PartitionIdGenerator;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

import software.amazon.aws.clients.swf.flux.step.PartitionedWorkflowStep;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Tests that validate Flux's behavior for partitioned workflows.
 */
public class PartitionedWorkflowTests extends WorkflowTestBase {
    private final static Logger log = LoggerFactory.getLogger(PartitionedWorkflowTests.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new PartitionedGreeting());
    }

    int getWorkerPoolThreadCount() {
        return 30; // we want the workflows to run as fast as reasonably possible
    }

    @Test
    public void testPartitionedWorkflowStepExecutesAllPartitions() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(PartitionedGreeting.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

        Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                            new HashSet<>(info.tagList()));

        Set<String> partitionIds = PartitionedStep.getGeneratedPartitionsByWorkflowId().get(uuid);
        Assert.assertEquals(partitionIds.size(), PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).size());
        for (String id : partitionIds) {
            Assert.assertTrue(PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).contains(id));
        }
    }

    @Test
    public void testPartitionedWorkflowStepExecutesAllPartitionsWithHighParallelism() throws InterruptedException {
        Set<String> uuids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            executeWorkflow(PartitionedGreeting.class, uuid, Collections.emptyMap());
        }


        for (String uuid : uuids) {
            WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

            Assert.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

            Set<String> partitionIds = PartitionedStep.getGeneratedPartitionsByWorkflowId().get(uuid);
            Assert.assertEquals(partitionIds.size(), PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).size());
            for (String id : partitionIds) {
                Assert.assertTrue(PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).contains(id));
            }
        }
    }

    /**
     * Basic workflow with one partitioned step.
     */
    public static final class PartitionedGreeting implements Workflow {
        private final WorkflowGraph graph;

        PartitionedGreeting() {
            WorkflowStep stepOne = new PartitionedStep();
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
     * Simple step that records which workflow and partition IDs the step is executed for.
     */
    public static final class PartitionedStep implements PartitionedWorkflowStep {
        private static final Map<String, Set<String>> generatedPartitionsByWorkflowId
                = Collections.synchronizedMap(new HashMap<>());
        private static final Map<String, Set<String>> executedPartitionsByWorkflowId
                = Collections.synchronizedMap(new HashMap<>());

        @StepApply
        public void doThing(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId,
                            @Attribute(StepAttributes.PARTITION_ID) String partitionId) {
            executedPartitionsByWorkflowId.get(workflowId).add(partitionId);
        }

        /**
         * Generates 10 partition IDs and stores them in the generatedPartitionsByWorkflowId map.
         * Also inserts an empty set into the executedPartitionsByWorkflowId map.
         */
        @PartitionIdGenerator
        public List<String> generatePartitionIds(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                ids.add(UUID.randomUUID().toString());
            }
            executedPartitionsByWorkflowId.put(workflowId, Collections.synchronizedSet(new HashSet<>()));
            generatedPartitionsByWorkflowId.put(workflowId, ids);
            return new LinkedList<>(ids);
        }

        public static Map<String, Set<String>> getGeneratedPartitionsByWorkflowId() {
            return generatedPartitionsByWorkflowId;
        }

        public static Map<String, Set<String>> getExecutedPartitionsByWorkflowId() {
            return executedPartitionsByWorkflowId;
        }
    }
}
