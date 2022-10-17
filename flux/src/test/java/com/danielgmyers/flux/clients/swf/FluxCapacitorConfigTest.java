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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FluxCapacitorConfigTest {

    @Test
    public void disallowNullAwsRegion() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setAwsRegion(null));
    }

    @Test
    public void disallowNullExponentialBackoffBase() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setExponentialBackoffBase(null));
    }

    @Test
    public void disallowExponentialBackoffBaseLessThanOne() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setExponentialBackoffBase(0.75));
    }

    @Test
    public void disallowNullHostnameTransformer() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setHostnameTransformerForPollerIdentity(null));
    }

    @Test
    public void disallowNullSwfDomain() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setSwfDomain(null));
    }

    @Test
    public void disallowNullSwfEndpoint() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> config.setSwfEndpoint(null));
    }

    @Test
    public void gettingTaskListConfigForUnconfiguredTaskListGivesDefaultConfig() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();

        Assertions.assertEquals(new TaskListConfig(), config.getTaskListConfig("random-task-list-name"));
    }

    @Test
    public void getRetrievesPreviouslyPutTaskListConfig() {
        TaskListConfig taskListConfig = new TaskListConfig();
        taskListConfig.setDecisionTaskThreadCount(42);

        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.putTaskListConfig("random-task-list-name", taskListConfig);

        Assertions.assertEquals(taskListConfig, config.getTaskListConfig("random-task-list-name"));
    }
}
