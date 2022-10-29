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
import org.junit.jupiter.api.Assertions;

public class TestPartitionedStepUsesPartitionIdGeneratorResult implements PartitionedWorkflowStep {

    public static final String PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE = "huzzahForPartitionIdGenerators";
    public static final String PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE = "hooray";

    private final Set<String> partitionIds;

    public TestPartitionedStepUsesPartitionIdGeneratorResult(Set<String> partitionIds) {
        this.partitionIds = new HashSet<>(partitionIds);
    }

    @StepApply
    public void apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                      @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount,
                      @Attribute(PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE) String generatorAttribute) {
        Assertions.assertNotNull(partitionId);
        Assertions.assertEquals(partitionIds.size(), partitionCount.longValue());
        Assertions.assertEquals(PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE, generatorAttribute);
    }

    @PartitionIdGenerator
    public PartitionIdGeneratorResult partitionIds() {
        return PartitionIdGeneratorResult.create(partitionIds).withAttribute(PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE,
                                                                             PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE);
    }

    @Override
    public Map<String, Class<?>> declaredOutputAttributes() {
        Map<String, Class<?>> attrs = new HashMap<>();
        attrs.put(PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE, PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE.getClass());
        return attrs;
    }
}
