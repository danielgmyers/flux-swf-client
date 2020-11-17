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

package software.amazon.aws.clients.swf.flux;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Container for configuration data used by Flux at runtime.
 */
public class FluxCapacitorConfig {

    private String awsRegion;
    private Double exponentialBackoffBase;
    private Function<String, String> hostnameTransformerForPollerIdentity = (hostname) -> hostname;
    private String swfEndpoint;
    private String swfDomain;
    private final Map<String, TaskListConfig> taskListConfigs = new HashMap<>();

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
     * Please retrieve this data using getTaskListConfig.
     */
    @Deprecated
    public Map<String, Integer> getTaskListWorkerThreadCount() {
        return taskListConfigs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getActivityTaskThreadCount()));
    }

    /**
     * Please configure using putTaskListConfig.
     */
    @Deprecated
    public void setTaskListWorkerThreadCount(Map<String, Integer> taskListWorkerThreadCount) {
        if (taskListWorkerThreadCount != null) {
            for (Map.Entry<String, Integer> entry : taskListWorkerThreadCount.entrySet()) {
                getTaskListConfig(entry.getKey()).setActivityTaskThreadCount(entry.getValue());
            }
        }
    }

    /**
     * Please retrieve this data using getTaskListConfig.
     */
    @Deprecated
    public Map<String, Integer> getTaskListActivityTaskPollerThreadCount() {
        return taskListConfigs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getActivityTaskPollerThreadCount()));
    }

    /**
     * Please configure using putTaskListConfig.
     */
    @Deprecated
    public void setTaskListActivityTaskPollerThreadCount(Map<String, Integer> taskListActivityTaskPollerThreadCount) {
        if (taskListActivityTaskPollerThreadCount != null) {
            for (Map.Entry<String, Integer> entry : taskListActivityTaskPollerThreadCount.entrySet()) {
                getTaskListConfig(entry.getKey()).setActivityTaskPollerThreadCount(entry.getValue());
            }
        }
    }

    /**
     * Please retrieve this data using getTaskListConfig.
     */
    @Deprecated
    public Map<String, Integer> getTaskListDeciderThreadCount() {
        return taskListConfigs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDecisionTaskThreadCount()));
    }

    /**
     * Please configure using putTaskListConfig.
     */
    @Deprecated
    public void setTaskListDeciderThreadCount(Map<String, Integer> taskListDeciderThreadCount) {
        if (taskListDeciderThreadCount != null) {
            for (Map.Entry<String, Integer> entry : taskListDeciderThreadCount.entrySet()) {
                getTaskListConfig(entry.getKey()).setDecisionTaskThreadCount(entry.getValue());
            }
        }
    }

    /**
     * Please retrieve this data using getTaskListConfig.
     */
    @Deprecated
    public Map<String, Integer> getTaskListDecisionTaskPollerThreadCount() {
        return taskListConfigs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getDecisionTaskPollerThreadCount()));
    }

    /**
     * Please configure using putTaskListConfig.
     */
    @Deprecated
    public void setTaskListDecisionTaskPollerThreadCount(Map<String, Integer> taskListDecisionTaskPollerThreadCount) {
        if (taskListDecisionTaskPollerThreadCount != null) {
            for (Map.Entry<String, Integer> entry : taskListDecisionTaskPollerThreadCount.entrySet()) {
                getTaskListConfig(entry.getKey()).setDecisionTaskPollerThreadCount(entry.getValue());
            }
        }
    }

    /**
     * Please retrieve this data using getTaskListConfig.
     */
    @Deprecated
    public Map<String, Integer> getTaskListPeriodicSubmitterThreadCount() {
        return taskListConfigs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPeriodicSubmitterThreadCount()));
    }

    /**
     * Please configure using putTaskListConfig.
     */
    @Deprecated
    public void setTaskListPeriodicSubmitterThreadCount(Map<String, Integer> taskListPeriodicSubmitterThreadCount) {
        if (taskListPeriodicSubmitterThreadCount != null) {
            for (Map.Entry<String, Integer> entry : taskListPeriodicSubmitterThreadCount.entrySet()) {
                getTaskListConfig(entry.getKey()).setPeriodicSubmitterThreadCount(entry.getValue());
            }
        }
    }
}
