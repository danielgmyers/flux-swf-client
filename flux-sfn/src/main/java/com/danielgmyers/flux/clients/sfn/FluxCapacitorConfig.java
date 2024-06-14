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

import java.util.function.Function;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/**
 * Container for configuration data used by Flux at runtime.
 */
public class FluxCapacitorConfig {

    private String awsRegion;
    private Double exponentialBackoffBase;
    private Function<String, String> hostnameTransformerForPollerIdentity = (hostname) -> hostname;
    private String sfnEndpoint;
    private ClientOverrideConfiguration clientOverrideConfiguration;
    private Function<String, RemoteSfnClientConfig> remoteSfnClientConfigProvider;

    public String getAwsRegion() {
        return awsRegion;
    }

    /**
     * Configures the AWS region that Flux uses to communicate with Step Functions.
     *
     * This value is required.
     */
    public void setAwsRegion(String awsRegion) {
        if (awsRegion == null) {
            throw new IllegalArgumentException("awsRegion may not be null.");
        }
        this.awsRegion = awsRegion;
    }

    public Double getExponentialBackoffBase() {
        return exponentialBackoffBase;
    }

    /**
     * This overrides the base used to calculate the exponential backoff duration, which defaults to 1.25.
     * Even if this value is set, individual steps can still use a different base via the @StepApply annotation.
     *
     * If set, this value must not be less than 1.
     */
    public void setExponentialBackoffBase(Double exponentialBackoffBase) {
        if (exponentialBackoffBase == null) {
            throw new IllegalArgumentException("exponentialBackoffBase may not be null.");
        }
        if (exponentialBackoffBase < 1.0) {
            throw new IllegalArgumentException("exponentialBackoffBase must be at least 1.");
        }
        this.exponentialBackoffBase = exponentialBackoffBase;
    }

    public Function<String, String> getHostnameTransformerForPollerIdentity() {
        return hostnameTransformerForPollerIdentity;
    }

    /**
     * If specified, Flux will use the provided function to transform the hostname prior to using it to poll for work.
     */
    public void setHostnameTransformerForPollerIdentity(Function<String, String> hostnameTransformerForPollerIdentity) {
        if (hostnameTransformerForPollerIdentity == null) {
            throw new IllegalArgumentException("hostnameTransformerForPollerIdentity may not be null.");
        }
        this.hostnameTransformerForPollerIdentity = hostnameTransformerForPollerIdentity;
    }

    public String getSfnEndpoint() {
        return sfnEndpoint;
    }

    /**
     * Sets the endpoint used to communicate with Step Functions.
     *
     * This configuration value is optional; if not specified, the endpoint will be determined automatically
     * using the awsRegion configuration value.
     *
     * Note that the provided endpoint value must begin with a valid URI scheme, which should most likely be "https://".
     */
    public void setSfnEndpoint(String sfnEndpoint) {
        if (sfnEndpoint == null) {
            throw new IllegalArgumentException("sfnEndpoint may not be null.");
        }
        this.sfnEndpoint = sfnEndpoint;
    }

    public ClientOverrideConfiguration getClientOverrideConfiguration() {
        return clientOverrideConfiguration;
    }

    /**
     * Overrides the default configuration used by the SfnClient created by Flux.
     *
     * Note that by default, Flux already overrides the client's default retry policy by disabling it
     * entirely, and implements its own retry and backoff logic, in order to generate clean metrics.
     * However, if this override configuration specifies a retry policy, then Flux will not disable
     * retries, but Flux's Step Functions API metrics will no longer reflect only durations for single API calls.
     */
    public void setClientOverrideConfiguration(ClientOverrideConfiguration clientOverrideConfiguration) {
        this.clientOverrideConfiguration = clientOverrideConfiguration;
    }

    /**
     * Specifies a callback function that Flux can use to retrieve configuration for a RemoteWorkflowExecutor.
     * The input to this callback is an arbitrary "endpoint id". This string is provided by the user
     * as input to FluxCapacitor.getRemoteWorkflowExecutor().
     * For example, the user might choose to map the endpoint id "standby-region" to a particular remote region config.
     */
    public void setRemoteSfnClientConfigProvider(Function<String, RemoteSfnClientConfig> remoteSfnClientConfigProvider) {
        this.remoteSfnClientConfigProvider = remoteSfnClientConfigProvider;
    }

    public Function<String, RemoteSfnClientConfig> getRemoteSfnClientConfigProvider() {
        return remoteSfnClientConfigProvider;
    }
}
