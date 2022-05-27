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

package software.amazon.aws.clients.swf.flux.poller.testwf;

import software.amazon.aws.clients.swf.flux.step.CloseWorkflow;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder;

public class TestWorkflowWithFailureTransition implements Workflow {

    private WorkflowGraph graph;

    public TestWorkflowWithFailureTransition() {
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(new TestStepOne());
        WorkflowStep stepTwo = new TestStepTwo();
        builder.addStep(stepTwo);

        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.failTransition(TestStepOne.class, CloseWorkflow.class);

        builder.alwaysClose(stepTwo);

        graph = builder.build();
    }

    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
