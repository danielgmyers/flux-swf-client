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
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.sfn.SfnClient;

/**
 * Real implementation of the RemoteWorkflowExecutor interface.
 */
public class RemoteWorkflowExecutorImpl implements RemoteWorkflowExecutor {

    private static final Logger log = LoggerFactory.getLogger(RemoteWorkflowExecutorImpl.class);

    private final Clock clock;
    private final MetricRecorderFactory metricsFactory;
    private final Map<String, Workflow> workflowsByName;
    private final SfnClient sfn;
    private final RemoteSfnClientConfig config;

    RemoteWorkflowExecutorImpl(Clock clock, MetricRecorderFactory metricsFactory, Map<String, Workflow> workflowsByName,
                               SfnClient sfn, RemoteSfnClientConfig config) {
        this.clock = clock;
        this.metricsFactory = metricsFactory;
        this.sfn = sfn;
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
        return null;
    }
}
