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

import java.util.Arrays;
import java.util.Collection;

import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

public class TestWorkflowWithPartitionedStep implements Workflow {

    private WorkflowGraph graph;

    public TestWorkflowWithPartitionedStep() {
        this(Arrays.asList("1", "2"));
    }

    public TestWorkflowWithPartitionedStep(Collection<String> partitionIds) {
        TestStepOne stepOne = new TestStepOne();
        TestPartitionedStep partitionedStep = new TestPartitionedStep(partitionIds);
        TestStepHasOptionalInputAttribute stepThree = new TestStepHasOptionalInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(stepOne);
        builder.alwaysTransition(stepOne, partitionedStep);

        builder.addStep(partitionedStep);
        builder.successTransition(partitionedStep, stepThree);
        builder.closeOnFailure(partitionedStep);

        builder.addStep(stepThree);
        builder.alwaysClose(stepThree);

        this.graph = builder.build();

        graph = builder.build();
    }

    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
