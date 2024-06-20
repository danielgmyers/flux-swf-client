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
import java.util.function.Function;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;

/**
 * The primary class through which the Flux library is used at runtime.
 */
public class FluxCapacitorImpl implements FluxCapacitor {

    private static final Logger log = LoggerFactory.getLogger(FluxCapacitorImpl.class);

    private final SfnClient sfn;
    private final FluxCapacitorConfig config;
    private final MetricRecorderFactory metricsFactory;
    private final Clock clock;

    private final Map<Class<? extends Workflow>, Workflow> workflowsByClass;
    private final Map<Class<? extends WorkflowStep>, WorkflowStep> activitiesByClass;

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

        this.workflowsByClass = new HashMap<>();
        this.activitiesByClass = new HashMap<>();
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
        // For the regular FluxCapacitor SfnClient, we disabled all the retry logic
        // since we do our own for metrics purposes. However, the remote client is not used
        // for much, and we don't bother emitting metrics for it, so the defaults are fine.

        Function<String, RemoteSfnClientConfig> remoteConfigProvider = config.getRemoteSfnClientConfigProvider();
        if (remoteConfigProvider == null) {
            throw new IllegalStateException("Cannot create a remote workflow executor without a remote client config provider.");
        }
        RemoteSfnClientConfig remoteConfig = remoteConfigProvider.apply(endpointId);

        AwsCredentialsProvider credentials = remoteConfig.getCredentials();
        if (credentials == null) {
            credentials = DefaultCredentialsProvider.create();
        }

        ClientOverrideConfiguration overrideConfig = remoteConfig.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder().build();
        }

        SfnClientBuilder customSfn = SfnClient.builder().credentialsProvider(credentials)
                .region(Region.of(remoteConfig.getAwsRegion()))
                .overrideConfiguration(overrideConfig);
        if (remoteConfig.getSfnEndpoint() != null) {
            customSfn.endpointOverride(URI.create(remoteConfig.getSfnEndpoint()));
        }
        return new RemoteWorkflowExecutorImpl(clock, metricsFactory, workflowsByClass, customSfn.build(), config, remoteConfig);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    // Package-private for RemoteWorkflowExecutorImpl and unit test access
    static StartExecutionRequest buildStartWorkflowRequest(Workflow workflow, String awsRegion, String awsAccountId,
                                                           String executionId, Map<String, Object> input) {
        return StartExecutionRequest.builder()
                .input(StepAttributes.encode(StepAttributes.serializeMapValues(input)))
                .name(executionId)
                .stateMachineArn(SfnArnFormatter.workflowArn(awsRegion, awsAccountId, workflow.getClass()))
                .build();
    }
}
