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

package software.amazon.aws.clients.swf.flux.wf.graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

/**
 * Represents a single workflow step and its possible step transitions in the graph.
 */
public class WorkflowGraphNodeImpl implements WorkflowGraphNode {

    private final WorkflowStep step;
    private final Map<String, WorkflowGraphNode> nextStepsByResultCode;

    // package-private so only WorkflowGraphBuilder can create one
    WorkflowGraphNodeImpl(WorkflowStep step) {
        this.step = step;
        this.nextStepsByResultCode = new HashMap<>();
    }

    // only for WorkflowGraphBuilder internal use. If node is null, the workflow will end.
    @Override
    public void addTransition(String resultCode, WorkflowGraphNode node) {
        nextStepsByResultCode.put(resultCode, node);
    }

    @Override
    public WorkflowStep getStep() {
        return step;
    }

    @Override
    public Map<String, WorkflowGraphNode> getNextStepsByResultCode() {
        return Collections.unmodifiableMap(nextStepsByResultCode);
    }
}
