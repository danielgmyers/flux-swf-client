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

package software.amazon.aws.clients.swf.flux;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.poller.TaskNaming;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionAlreadyStartedException;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;

/**
 * Real implementation of the RemoteWorkflowExecutor interface.
 */
public class RemoteWorkflowExecutorImpl implements RemoteWorkflowExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowExecutorImpl.class);

    private final MetricRecorderFactory metricsFactory;
    private final Map<String, Workflow> workflowsByName;
    private final SwfClient swf;
    private final FluxCapacitorConfig config;

    RemoteWorkflowExecutorImpl(MetricRecorderFactory metricsFactory, Map<String, Workflow> workflowsByName,
                               SwfClient swf, FluxCapacitorConfig config) {
        this.metricsFactory = metricsFactory;
        this.swf = swf;
        this.config = config;
        this.workflowsByName = Collections.unmodifiableMap(workflowsByName);
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return executeWorkflow(workflowType, workflowId, workflowInput, Collections.emptySet());
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        String workflowName = TaskNaming.workflowName(workflowType);
        if (!workflowsByName.containsKey(workflowName)) {
            throw new WorkflowExecutionException("Cannot execute a workflow that was not provided to Flux at initialization: "
                                                 + workflowName);
        }

        Workflow workflow = workflowsByName.get(workflowName);

        Set<String> actualExecutionTags = new HashSet<>(executionTags);
        if (config.getAutomaticallyTagExecutionsWithTaskList() == null
            || config.getAutomaticallyTagExecutionsWithTaskList()) {
            actualExecutionTags.add(workflow.taskList());
        }

        StartWorkflowExecutionRequest request
                = FluxCapacitorImpl.buildStartWorkflowRequest(config.getSwfDomain(), workflowName, workflowId,
                                                              workflow.taskList(), workflow.maxStartToCloseDuration(),
                                                              workflowInput, actualExecutionTags);

        log.debug("Requesting new remote workflow execution for workflow {} with id {}", workflowName, workflowId);

        try {
            StartWorkflowExecutionResponse workflowRun = swf.startWorkflowExecution(request);
            log.debug("Started remote workflow {} with id {}: received execution id {}.",
                      workflowName, workflowId, workflowRun.runId());

            return new WorkflowStatusCheckerImpl(swf, config.getSwfDomain(), workflowId, workflowRun.runId());
        } catch (WorkflowExecutionAlreadyStartedException e) {
            // swallow, we're ok with this happening
            log.debug("Attempted to start remote workflow {} with id {} but it was already started.",
                      workflowName, workflowId, e);

            // TODO - figure out how to get the execution id in this case, for now just always show an UNKNOWN status.
            return new WorkflowStatusChecker() {
                @Override
                public WorkflowStatus checkStatus() {
                    return WorkflowStatus.UNKNOWN;
                }

                @Override
                public WorkflowExecutionInfo getExecutionInfo() {
                    return null;
                }

                public SwfClient getSwfClient() {
                    return swf;
                }
            };
        } catch (Exception e) {
            String message = String.format("Got exception attempting to start remote workflow %s with id %s",
                                           workflowName, workflowId);
            log.debug(message, e);
            throw new WorkflowExecutionException(message, e);
        }
    }
}
