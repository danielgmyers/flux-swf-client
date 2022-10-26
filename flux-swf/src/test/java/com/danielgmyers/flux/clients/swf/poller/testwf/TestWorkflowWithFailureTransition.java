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

import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;
import com.danielgmyers.flux.step.CloseWorkflow;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;

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
