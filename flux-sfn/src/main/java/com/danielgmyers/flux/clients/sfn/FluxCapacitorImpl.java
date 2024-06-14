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

import java.net.URI;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;

/**
 * The primary class through which the Flux library is used at runtime.
 */
public class FluxCapacitorImpl implements FluxCapacitor {

    private static final Logger log = LoggerFactory.getLogger(FluxCapacitorImpl.class);

    private final SfnClient sfn;
    private final FluxCapacitorConfig config;
    private final MetricRecorderFactory metricsFactory;
    private final Clock clock;

    private final Map<String, Workflow> workflowsByName;
    private final Map<String, WorkflowStep> activitiesByName;

    /**
     * Initializes a FluxCapacitor object. Package-private for unit test use.
     *
     * @param sfn    - The client that should be used for all Step Functions API calls
     * @param config - Config data used to configure FluxCapacitor behavior.
     */
    FluxCapacitorImpl(MetricRecorderFactory metricsFactory, SfnClient sfn, FluxCapacitorConfig config, Clock clock) {
        this.metricsFactory = metricsFactory;
        this.sfn = sfn;
        this.config = config;
        this.clock = clock;

        this.workflowsByName = new HashMap<>();
        this.activitiesByName = new HashMap<>();
    }

    /**
     * Creates a FluxCapacitor object and does various bits of setup.
     * Intentionally package-private, only the Factory should be using the constructor.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param credentials    - A provider for the AWS credentials that should be used to call AWS APIs
     * @param config         - Configuration data for FluxCapacitor to use to configure itself
     */
    static FluxCapacitor create(MetricRecorderFactory metricsFactory, AwsCredentialsProvider credentials,
                                FluxCapacitorConfig config) {
        // We do our own retry/backoff logic so we can get decent metrics, so here we disable the SDK's defaults.
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .retryCondition(RetryCondition.none())
                .numRetries(0)
                .backoffStrategy(BackoffStrategy.none())
                .throttlingBackoffStrategy(BackoffStrategy.none())
                .build();

        // If an override config was provided, use it, and only use the above RetryPolicy
        // if the provided overrideConfig did not include its own RetryPolicy.
        ClientOverrideConfiguration overrideConfig = config.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(retryPolicy)
                    .build();
        } else if (overrideConfig.retryPolicy().isEmpty()) {
            overrideConfig = overrideConfig.toBuilder().retryPolicy(retryPolicy).build();
        }

        SfnClientBuilder builder = SfnClient.builder()
                .credentialsProvider(credentials)
                .region(Region.of(config.getAwsRegion()))
                .overrideConfiguration(overrideConfig);

        // if the user specified a custom endpoint, we'll use it;
        // otherwise the SDK will figure it out based on the region name.
        if (config.getSfnEndpoint() != null && !"".equals(config.getSfnEndpoint())) {
            builder.endpointOverride(URI.create(config.getSfnEndpoint()));
        }

        return new FluxCapacitorImpl(metricsFactory, builder.build(), config, Clock.systemUTC());
    }

    @Override
    public void initialize(List<Workflow> workflows) {

    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return null;
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        return null;
    }

    @Override
    public RemoteWorkflowExecutor getRemoteWorkflowExecutor(String endpointId) {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }
}
