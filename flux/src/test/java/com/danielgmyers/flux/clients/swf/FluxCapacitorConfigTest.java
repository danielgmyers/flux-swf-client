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

import org.junit.Assert;
import org.junit.Test;

public class FluxCapacitorConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void disallowNullAwsRegion() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowNullExponentialBackoffBase() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setExponentialBackoffBase(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowExponentialBackoffBaseLessThanOne() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setExponentialBackoffBase(0.75);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowNullHostnameTransformer() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setHostnameTransformerForPollerIdentity(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowNullSwfDomain() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setSwfDomain(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowNullSwfEndpoint() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setSwfEndpoint(null);
    }

    @Test
    public void gettingTaskListConfigForUnconfiguredTaskListGivesDefaultConfig() {
        FluxCapacitorConfig config = new FluxCapacitorConfig();

        Assert.assertEquals(new TaskListConfig(), config.getTaskListConfig("random-task-list-name"));
    }

    @Test
    public void getRetrievesPreviouslyPutTaskListConfig() {
        TaskListConfig taskListConfig = new TaskListConfig();
        taskListConfig.setDecisionTaskThreadCount(42);

        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.putTaskListConfig("random-task-list-name", taskListConfig);

        Assert.assertEquals(taskListConfig, config.getTaskListConfig("random-task-list-name"));
    }
}
