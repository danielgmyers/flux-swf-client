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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TaskListConfigTest {

    @Test
    public void defaultConfigValues() {
        TaskListConfig config = new TaskListConfig();
        Assertions.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_TASK_THREAD_COUNT, config.getActivityTaskThreadCount());
        Assertions.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_POLLER_THREAD_COUNT, config.getActivityPollerThreadCount());
    }

    @Test
    public void allowCustomValues() {
        TaskListConfig config = new TaskListConfig();
        config.setActivityTaskThreadCount(134);
        config.setActivityPollerThreadCount(42);

        Assertions.assertEquals(134, config.getActivityTaskThreadCount());
        Assertions.assertEquals(42, config.getActivityPollerThreadCount());
    }
}
