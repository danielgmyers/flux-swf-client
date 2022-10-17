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

package com.danielgmyers.flux.clients.swf.poller.testwf;

import java.util.Arrays;
import java.util.List;

import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGenerator;
import com.danielgmyers.flux.clients.swf.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.danielgmyers.flux.clients.swf.step.StepResult;

public class TestPartitionedStepWithExtraInput implements PartitionedWorkflowStep {

    public static final String INPUT_ATTR = "foo";

    @StepApply
    public StepResult apply(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                            @Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount,
                            @Attribute(INPUT_ATTR) String foo) {
        return StepResult.success();
    }

    @PartitionIdGenerator
    public List<String> partitionIds(@Attribute(INPUT_ATTR) String foo) {
        return Arrays.asList("1", "2");
    }
}
