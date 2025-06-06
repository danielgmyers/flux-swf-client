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

package com.danielgmyers.flux.clients.swf;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/**
 * Container for configuration data used by Flux at runtime.
 */
public class FluxCapacitorConfig {

    private String awsRegion;
    private Double exponentialBackoffBase;
    private Function<String, String> hostnameTransformerForPollerIdentity = (hostname) -> hostname;
    private String swfEndpoint;
    private String swfDomain;
    private ClientOverrideConfiguration clientOverrideConfiguration;
    private final Map<String, TaskListConfig> taskListConfigs = new HashMap<>();
    private Boolean automaticallyTagExecutionsWithTaskList;
    private Boolean enablePartitionIdHashing;
    private Function<String, RemoteSwfClientConfig> remoteSwfClientConfigProvider;

    public String getAwsRegion() {
        return awsRegion;
    }

    /**
     * Configures the AWS region that Flux uses to communicate with SWF.
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

    public String getSwfEndpoint() {
        return swfEndpoint;
    }

    /**
     * Sets the SWF endpoint used to communicate with SWF.
     *
     * This configuration value is optional; if not specified, the endpoint will be determined automatically
     * using the awsRegion configuration value.
     *
     * Note that the provided endpoint value must begin with a valid URI scheme, which should most likely be "https://".
     */
    public void setSwfEndpoint(String swfEndpoint) {
        if (swfEndpoint == null) {
            throw new IllegalArgumentException("swfEndpoint may not be null.");
        }
        this.swfEndpoint = swfEndpoint;
    }

    public String getSwfDomain() {
        return swfDomain;
    }

    /**
     * Configures the SWF domain name that this Flux instance's workflows will be scoped to.
     *
     * This value is required.
     */
    public void setSwfDomain(String swfDomain) {
        if (swfDomain == null) {
            throw new IllegalArgumentException("swfDomain may not be null.");
        }
        this.swfDomain = swfDomain;
    }

    public ClientOverrideConfiguration getClientOverrideConfiguration() {
        return clientOverrideConfiguration;
    }

    /**
     * Overrides the default configuration used by the SwfClient created by Flux.
     *
     * Note that by default, Flux already overrides the client's default retry policy by disabling it
     * entirely, and implements its own retry and backoff logic, in order to generate clean metrics.
     * However, if this override configuration specifies a retry policy, then Flux will not disable
     * retries, but Flux's SWF API metrics will no longer reflect only durations for single API calls.
     */
    public void setClientOverrideConfiguration(ClientOverrideConfiguration clientOverrideConfiguration) {
        this.clientOverrideConfiguration = clientOverrideConfiguration;
    }

    /**
     * Stores the provided task list configuration for the specified task list name.
     *
     * Task lists that are not explicitly configured will get the default task list configuration;
     * see TaskListConfig for more information on the default values.
     */
    public void putTaskListConfig(String taskListName, TaskListConfig config) {
        if (taskListName == null) {
            throw new IllegalArgumentException("taskListName may not be null.");
        }
        if (config == null) {
            throw new IllegalArgumentException("config may not be null.");
        }
        this.taskListConfigs.put(taskListName, config);
    }

    public TaskListConfig getTaskListConfig(String taskList) {
        return taskListConfigs.computeIfAbsent(taskList, name -> new TaskListConfig());
    }

    /**
     * Controls whether Flux automatically includes a workflow's task list name in the list of tags for new executions.
     *
     * This configuration value is optional; if not specified, this functionality will be enabled.
     */
    public void setAutomaticallyTagExecutionsWithTaskList(Boolean automaticallyTagExecutionsWithTaskList) {
        this.automaticallyTagExecutionsWithTaskList = automaticallyTagExecutionsWithTaskList;
    }

    public Boolean getAutomaticallyTagExecutionsWithTaskList() {
        return automaticallyTagExecutionsWithTaskList;
    }

    /**
     * Controls whether Flux will use a hash of the user-provided partition IDs when generating activity IDs.
     * If not specified, this flag is disabled.
     *
     * If disabled, user-provided partition IDs will be limited to a length of 123 characters, and may not contain ":", "/", or "|".
     * If enabled, user-provided partition IDs will be limited to a length of 256 characters, with no content constraints.
     */
    public void setEnablePartitionIdHashing(Boolean enablePartitionIdHashing) {
        this.enablePartitionIdHashing = enablePartitionIdHashing;
    }

    public boolean getEnablePartitionIdHashing() {
        return (enablePartitionIdHashing != null ? enablePartitionIdHashing : false);
    }

    /**
     * Specifies a callback function that Flux can use to retrieve configuration for a RemoteWorkflowExecutor.
     * The input to this callback is an arbitrary "endpoint id". This string is provided by the user
     * as input to FluxCapacitor.getRemoteWorkflowExecutor().
     * For example, the user might choose to map the endpoint id "standby-region" to a particular remote region config.
     */
    public void setRemoteSwfClientConfigProvider(Function<String, RemoteSwfClientConfig> remoteSwfClientConfigProvider) {
        this.remoteSwfClientConfigProvider = remoteSwfClientConfigProvider;
    }

    public Function<String, RemoteSwfClientConfig> getRemoteSwfClientConfigProvider() {
        return remoteSwfClientConfigProvider;
    }
}
