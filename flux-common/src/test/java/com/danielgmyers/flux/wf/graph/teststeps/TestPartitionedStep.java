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

package com.danielgmyers.flux.wf.graph.teststeps;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.metrics.MetricRecorder;
import org.junit.jupiter.api.Assertions;

public class TestPartitionedStep implements PartitionedWorkflowStep {

    public static final String PARTITION_ID_GENERATOR_METRIC = "partitionIdCountOrSomething";

    private final Set<String> partitionIds;
    private final Map<String, Object> additionalAttributes;

    public TestPartitionedStep() {
        this(Arrays.asList("1", "2"));
    }

    public TestPartitionedStep(Collection<String> partitionIds) {
        this(partitionIds, new HashMap<>());
    }

    public TestPartitionedStep(Collection<String> partitionIds, Map<String, Object> additionalAttributes) {
        this.partitionIds = new HashSet<>(partitionIds);
        this.additionalAttributes = additionalAttributes;
    }

    @StepApply
    public StepResult apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                            @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {
        Assertions.assertNotNull(partitionId);
        Assertions.assertEquals(partitionIds.size(), partitionCount.longValue());
        return StepResult.success();
    }

    @PartitionIdGenerator
    public PartitionIdGeneratorResult partitionIds(MetricRecorder metrics, @Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
        metrics.addCount(PARTITION_ID_GENERATOR_METRIC, partitionIds.size());
        metrics.addProperty("workflowIdForPartitionGenerator", workflowId);
        Assertions.assertNotNull(workflowId);
        return PartitionIdGeneratorResult.create(partitionIds).withAttributes(additionalAttributes);
    }

    public void setPartitionIds(Collection<String> partitionIds) {
        this.partitionIds.clear();
        this.partitionIds.addAll(partitionIds);
    }
}
