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

package com.danielgmyers.flux.clients.sfn;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.ex.WorkflowExecutionException;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ExecutionAlreadyExistsException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

/**
 * Real implementation of the RemoteWorkflowExecutor interface.
 */
public class RemoteWorkflowExecutorImpl implements RemoteWorkflowExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowExecutorImpl.class);

    private final Clock clock;
    private final MetricRecorderFactory metricsFactory;
    private final Map<Class<? extends Workflow>, Workflow> workflowsByClass;
    private final SfnClient sfn;
    private final FluxCapacitorConfig fluxCapacitorConfig;
    private final RemoteSfnClientConfig config;

    RemoteWorkflowExecutorImpl(Clock clock, MetricRecorderFactory metricsFactory,
                               Map<Class<? extends Workflow>, Workflow> workflowsByClass, SfnClient sfn,
                               FluxCapacitorConfig fluxCapacitorConfig, RemoteSfnClientConfig config) {
        this.clock = clock;
        this.metricsFactory = metricsFactory;
        this.sfn = sfn;
        this.fluxCapacitorConfig = fluxCapacitorConfig;
        this.config = config;
        this.workflowsByClass = Collections.unmodifiableMap(workflowsByClass);
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return executeWorkflow(workflowType, workflowId, workflowInput, Collections.emptySet());
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        String workflowName = workflowType.getSimpleName();
        if (!workflowsByClass.containsKey(workflowType)) {
            throw new WorkflowExecutionException("Cannot execute a workflow that was not provided to Flux at initialization: "
                    + workflowName);
        }
        if (executionTags != null && !executionTags.isEmpty()) {
            log.warn("Step Functions does not support workflow execution tags. Ignoring the provided tags: {} for workflow {}",
                     executionTags, workflowId);
        }

        String accountId = config.getAwsAccountId();
        if (accountId == null) {
            accountId = fluxCapacitorConfig.getAwsAccountId();
        }

        Workflow workflow = workflowsByClass.get(workflowType);
        StartExecutionRequest request = FluxCapacitorImpl.buildStartWorkflowRequest(workflow, config.getAwsRegion(),
                                                                                    accountId, workflowId, workflowInput);

        log.debug("Requesting new remote workflow execution for workflow {} with id {}", workflowName, workflowId);

        try {
            StartExecutionResponse response = sfn.startExecution(request);
            log.debug("Started remote workflow {} with id {}: received execution {}.",
                      workflowName, workflowId, response.executionArn());
            return new WorkflowStatusCheckerImpl(clock, sfn, response.executionArn());
        } catch (ExecutionAlreadyExistsException e) {
            // swallow, we're ok with this happening
            log.debug("Attempted to start remote workflow {} with id {} but it was already started.",
                      workflowName, workflowId, e);
            String executionArn = SfnArnFormatter.executionArn(config.getAwsRegion(), accountId, workflowType, workflowId);
            return new WorkflowStatusCheckerImpl(clock, sfn, executionArn);
        } catch (Exception e) {
            String message = String.format("Got exception attempting to start remote workflow %s with id %s",
                                           workflowName, workflowId);
            log.debug(message, e);
            throw new WorkflowExecutionException(message, e);
        }
    }
}
