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

package com.danielgmyers.flux.testutil;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.wf.Workflow;

/**
 * A stub FluxCapacitor implementation intended to be used by unit tests.
 */
public class StubFluxCapacitor implements FluxCapacitor {

    private boolean shutdown = false;

    private final Set<Class<? extends Workflow>> workflowTypes;

    private final Map<WorkflowExecutionRecord, Map<String, Object>> executedWorkflows;

    /**
     * Constructs a StubFluxCapacitor object, call initialize to make it aware of a list of workflows.
     */
    public StubFluxCapacitor() {
        this.workflowTypes = new HashSet<>();
        this.executedWorkflows = new HashMap<>();
    }

    @Override
    public void initialize(List<Workflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("The specified workflow list must not be empty.");
        }

        for (Workflow workflow : workflows) {
            if (workflowTypes.contains(workflow.getClass())) {
                String workflowName = workflow.getClass().getSimpleName();
                String message = "Received more than one Workflow object with the same class name: " + workflowName;
                throw new RuntimeException(message);
            }
            workflowTypes.add(workflow.getClass());
        }
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return executeWorkflow(workflowType, workflowId, workflowInput, Collections.emptySet());
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        if (shutdown) {
            throw new RuntimeException("Cannot use the FluxCapacitor after calling shutdown()!");
        }
        if (!workflowTypes.contains(workflowType)) {
            throw new RuntimeException("Unrecognized workflow type: " + workflowType.getSimpleName());
        }

        WorkflowExecutionRecord execution = new WorkflowExecutionRecord(workflowType, workflowId);
        // if the execution was already requested, do nothing.
        // SWF ignores the input in this case, so we'll do the same.
        if (!executedWorkflows.containsKey(execution)) {
            // make our own copy of the input map to prevent external mutation
            Map<String, Object> inputCopy = new HashMap<>();
            if (workflowInput != null) {
                inputCopy.putAll(workflowInput);
            }
            executedWorkflows.put(execution, Collections.unmodifiableMap(inputCopy));
        }

        // TODO -- make some way for the test to control what this returns
        return () -> null;
    }

    @Override
    public RemoteWorkflowExecutor getRemoteWorkflowExecutor(String endpointId) {
        return new StubRemoteWorkflowExecutor();
    }

    /**
     * Utility method allowing unit tests to verify that a workflow was executed with the expected id and input.
     * @param workflowType       - The type of workflow that was executed
     * @param expectedWorkflowId - The id of the workflow that was executed
     * @param expectedInput      - The input to the workflow that was executed
     */
    public void verifyWorkflowWasStarted(Class<? extends Workflow> workflowType, String expectedWorkflowId,
                                         Map<String, Object> expectedInput) {
        if (!workflowTypes.contains(workflowType)) {
            throw new RuntimeException("Unrecognized workflow type: " + workflowType.getSimpleName());
        }

        WorkflowExecutionRecord execution = new WorkflowExecutionRecord(workflowType, expectedWorkflowId);
        if (!executedWorkflows.containsKey(execution)) {
            throw new RuntimeException(String.format("No %s execution request for %s was found.",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId));
        }
        if (expectedInput == null && !executedWorkflows.get(execution).isEmpty()) {
            throw new RuntimeException(String.format("%s execution %s should have had empty input but instead had %s",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId,
                                                     StepAttributes.encode(executedWorkflows.get(execution))));
        }
        if (!executedWorkflows.get(execution).equals(expectedInput)) {
            throw new RuntimeException(String.format("%s execution %s should have had input %s but instead had %s",
                                                     TaskNaming.workflowName(workflowType), expectedWorkflowId,
                                                     StepAttributes.encode(expectedInput),
                                                     StepAttributes.encode(executedWorkflows.get(execution))));
        }
    }

    /**
     * Utility method allowing unit tests to verify that a workflow was *not* executed with the specified id.
     * @param workflowType - The type of workflow that should not have been executed
     * @param workflowId   - The id of the workflow that should not have been executed
     */
    public void verifyWorkflowWasNotStarted(Class<? extends Workflow> workflowType, String workflowId) {
        WorkflowExecutionRecord execution = new WorkflowExecutionRecord(workflowType, workflowId);
        if (workflowTypes.contains(workflowType) && executedWorkflows.containsKey(execution)) {
            throw new RuntimeException(String.format("%s execution %s should not have been started.",
                                                     workflowType.getSimpleName(), workflowId));
        }
    }

    /**
     * Returns the number of unique workflows that were executed.
     * @return The count
     */
    public int countExecutedWorkflows() {
        return executedWorkflows.size();
    }

    /**
     * Resets the executed workflow cache, useful for preventing tests from interfering with each other.
     */
    public void resetExecutionCache() {
        executedWorkflows.clear();
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }
}
