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

import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;

public class TestWorkflowWithAlwaysTransition implements Workflow {

    private WorkflowGraph graph;

    public TestWorkflowWithAlwaysTransition() {
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(new TestStepOne());
        builder.alwaysTransition(TestStepOne.class, TestStepTwo.class);

        WorkflowStep two = new TestStepTwo();
        builder.addStep(two);
        builder.alwaysClose(two);

        graph = builder.build();
    }

    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
