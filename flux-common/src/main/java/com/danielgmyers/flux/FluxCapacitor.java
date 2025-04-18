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

package com.danielgmyers.flux;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.wf.Workflow;

/**
 * The primary interface through which the Flux library is used at runtime.
 */
public interface FluxCapacitor {

    /**
     * Initializes the library, registering the provided workflows and starting up the worker threads.
     * @param workflows The list of workflows that should be registered and polled.
     */
    void initialize(List<Workflow> workflows);

    /**
     * Executes a workflow of the specified type, using the specified workflow ID and input map.
     * Succeeds if a workflow with the specified workflow ID was already started.
     *
     * Returns a WorkflowStatusChecker object which can be used to monitor the status of the requested workflow.
     *
     * @param workflowType The class of the workflow which should be run
     * @param workflowId A unique identifier for the workflow execution (SWF dedupes on this)
     * @param workflowInput The map of input values for the workflow execution
     */
    WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType,
                                          String workflowId, Map<String, Object> workflowInput);

    /**
     * Executes a workflow of the specified type, using the specified workflow ID and input map.
     * Succeeds if a workflow with the specified workflow ID was already started.
     * Tags the new workflow execution with the provided set of execution tags.
     *
     * Returns a WorkflowStatusChecker object which can be used to monitor the status of the requested workflow.
     *
     * @param workflowType The class of the workflow which should be run
     * @param workflowId A unique identifier for the workflow execution (SWF dedupes on this)
     * @param workflowInput The map of input values for the workflow execution
     */
    WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType,
                                          String workflowId, Map<String, Object> workflowInput,
                                          Set<String> executionTags);

    /**
     * Returns an object that can submit workflow executions to another workflow service endpoint.
     * The FluxCapacitor implementation will require one or more callbacks to be provided via configuration
     * to look up the necessary information to initialize the underlying client.
     *
     * @param endpointId This should be an identifier that can be used by the aforementioned configuration callbacks to determine
     *                   which configuration data to supply to the underlying client.
     */
    RemoteWorkflowExecutor getRemoteWorkflowExecutor(String endpointId);

    /**
     * Shuts down this FluxCapacitor object's worker thread pools. Running threads are not interrupted.
     * Once you call this method, this FluxCapacitor object should not be used anymore,
     * a new one should be created with FluxCapacitorFactory.create instead.
     *
     * FluxCapacitor will likely take about 60 seconds to shut down, since that's the long-polling duration.
     *
     * This method obeys the ExecutorService.shutdown() contract.
     */
    void shutdown();

    /**
     * This method can optionally be called after shutdown() is called.
     * It will wait for any still-running worker threads to finish their tasks before returning.
     *
     * Returns true if everything terminated before the timeout, or false if there were still running tasks at that time.
     *
     * This method obeys the ExecutorService.awaitTermination(long timeout, TimeUnit unit) contract.
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
