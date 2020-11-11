/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.aws.clients.swf.flux.poller.testwf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.step.PartitionIdGenerator;
import software.amazon.aws.clients.swf.flux.step.PartitionIdGeneratorResult;
import software.amazon.aws.clients.swf.flux.step.PartitionedWorkflowStep;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;

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
        Assert.assertNotNull(partitionId);
        Assert.assertEquals(partitionIds.size(), partitionCount.longValue());
        Assert.assertEquals(PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE_VALUE, generatorAttribute);
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
