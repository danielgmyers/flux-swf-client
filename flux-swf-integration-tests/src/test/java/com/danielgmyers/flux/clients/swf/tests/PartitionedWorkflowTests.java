package com.danielgmyers.flux.clients.swf.tests;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGenerator;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.clients.swf.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Tests that validate Flux's behavior for partitioned workflows.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class PartitionedWorkflowTests extends WorkflowTestBase {
    private final static Logger log = LoggerFactory.getLogger(PartitionedWorkflowTests.class);

    @Override
    List<Workflow> getWorkflowsForTest() {
        return Collections.singletonList(new PartitionedGreeting());
    }

    @Override
    Logger getLogger() {
        return log;
    }

    int getWorkerPoolThreadCount() {
        return 30; // we want the workflows to run as fast as reasonably possible
    }

    @Test
    public void testPartitionedWorkflowStepExecutesAllPartitions() throws InterruptedException {
        String uuid = UUID.randomUUID().toString();
        executeWorkflow(PartitionedGreeting.class, uuid, Collections.emptyMap());
        WorkflowExecutionInfo info = waitForWorkflowCompletion(uuid, Duration.ofSeconds(60));

        Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

        Set<String> partitionIds = PartitionedStep.getGeneratedPartitionsByWorkflowId().get(uuid);
        Assertions.assertEquals(partitionIds.size(), PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).size());
        for (String id : partitionIds) {
            Assertions.assertTrue(PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).contains(id));
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

            Assertions.assertEquals(Collections.singleton(Workflow.DEFAULT_TASK_LIST_NAME),
                                new HashSet<>(info.tagList()));

            Set<String> partitionIds = PartitionedStep.getGeneratedPartitionsByWorkflowId().get(uuid);
            Assertions.assertEquals(partitionIds.size(), PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).size());
            for (String id : partitionIds) {
                Assertions.assertTrue(PartitionedStep.getExecutedPartitionsByWorkflowId().get(uuid).contains(id));
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
        public PartitionIdGeneratorResult generatePartitionIds(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                ids.add(UUID.randomUUID().toString());
            }
            executedPartitionsByWorkflowId.put(workflowId, Collections.synchronizedSet(new HashSet<>()));
            generatedPartitionsByWorkflowId.put(workflowId, ids);
            return PartitionIdGeneratorResult.create(ids);
        }

        public static Map<String, Set<String>> getGeneratedPartitionsByWorkflowId() {
            return generatedPartitionsByWorkflowId;
        }

        public static Map<String, Set<String>> getExecutedPartitionsByWorkflowId() {
            return executedPartitionsByWorkflowId;
        }
    }
}
