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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.step.Attribute;
import software.amazon.aws.clients.swf.flux.step.PartitionIdGenerator;
import software.amazon.aws.clients.swf.flux.step.PartitionedWorkflowStep;
import software.amazon.aws.clients.swf.flux.step.StepApply;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.StepResult;

public class TestPartitionedStep implements PartitionedWorkflowStep {

    public static final String PARTITION_ID_GENERATOR_METRIC = "partitionIdCountOrSomething";

    private final List<String> partitionIds;

    public TestPartitionedStep() {
        this(Arrays.asList("1", "2"));
    }

    public TestPartitionedStep(List<String> partitionIds) {
        this.partitionIds = new ArrayList<>(partitionIds);
    }

    @StepApply
    public StepResult apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                            @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {
        Assert.assertNotNull(partitionId);
        Assert.assertEquals(partitionIds.size(), partitionCount.longValue());
        return StepResult.success();
    }

    @PartitionIdGenerator
    public List<String> partitionIds(MetricRecorder metrics, @Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
        metrics.addCount(PARTITION_ID_GENERATOR_METRIC, partitionIds.size());
        metrics.addProperty("workflowIdForPartitionGenerator", workflowId);
        Assert.assertNotNull(workflowId);
        return partitionIds;
    }

    public void setPartitionIds(List<String> partitionIds) {
        this.partitionIds.clear();
        this.partitionIds.addAll(partitionIds);
    }
}
