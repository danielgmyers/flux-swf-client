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

package software.amazon.aws.clients.swf.flux;

import org.junit.Assert;
import org.junit.Test;

public class TaskListConfigTest {

    @Test
    public void defaultConfigValues() {
        TaskListConfig config = new TaskListConfig();
        Assert.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_TASK_THREAD_COUNT, config.getActivityTaskThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_TASK_THREAD_COUNT / 2, config.getActivityTaskPollerThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_BUCKET_COUNT, config.getBucketCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_DECISION_TASK_THREAD_COUNT, config.getDecisionTaskThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_DECISION_TASK_THREAD_COUNT / 2, config.getDecisionTaskPollerThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_PERIODIC_SUBMITTER_THREAD_COUNT, config.getPeriodicSubmitterThreadCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowZeroBucketCount() {
        TaskListConfig config = new TaskListConfig();
        config.setBucketCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void disallowNegativeBucketCount() {
        TaskListConfig config = new TaskListConfig();
        config.setBucketCount(-7);
    }

    @Test
    public void activityTaskPollerCountDefaultBehavior() {
        TaskListConfig config = new TaskListConfig();

        // if nothing is set, should equal half of the default task thread count
        Assert.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_TASK_THREAD_COUNT, config.getActivityTaskThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_ACTIVITY_TASK_THREAD_COUNT / 2,
                            config.getActivityTaskPollerThreadCount());

        // if activity task count is set, activity task poller count should equal half of that
        config.setActivityTaskThreadCount(42);
        Assert.assertEquals(21, config.getActivityTaskPollerThreadCount());

        // if activity task count is to 1, poller count should equal 1
        config.setActivityTaskThreadCount(1);
        Assert.assertEquals(1, config.getActivityTaskPollerThreadCount());

        // if poller count is explicitly set, it the activity task count shouldn't matter
        config.setActivityTaskThreadCount(42);
        config.setActivityTaskPollerThreadCount(33);
        Assert.assertEquals(33, config.getActivityTaskPollerThreadCount());
    }

    @Test
    public void decisionTaskPollerCountDefaultBehavior() {
        TaskListConfig config = new TaskListConfig();

        // if nothing is set, should equal half of the default task thread count
        Assert.assertEquals(TaskListConfig.DEFAULT_DECISION_TASK_THREAD_COUNT, config.getDecisionTaskThreadCount());
        Assert.assertEquals(TaskListConfig.DEFAULT_DECISION_TASK_THREAD_COUNT / 2,
                            config.getDecisionTaskPollerThreadCount());

        // if decision task count is set, decision task poller count should equal half of that
        config.setDecisionTaskThreadCount(42);
        Assert.assertEquals(21, config.getDecisionTaskPollerThreadCount());

        // if decision task count is to 1, poller count should equal 1
        config.setDecisionTaskThreadCount(1);
        Assert.assertEquals(1, config.getDecisionTaskPollerThreadCount());

        // if poller count is explicitly set, it the decision task count shouldn't matter
        config.setDecisionTaskThreadCount(42);
        config.setDecisionTaskPollerThreadCount(33);
        Assert.assertEquals(33, config.getDecisionTaskPollerThreadCount());
    }
}
