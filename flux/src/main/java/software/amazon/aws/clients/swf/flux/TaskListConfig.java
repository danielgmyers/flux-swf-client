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

import java.util.Objects;

/**
 * Container for task-list-specific configuration data used by Flux at runtime.
 */
public class TaskListConfig {

    // Visible for testing
    static final int DEFAULT_ACTIVITY_TASK_THREAD_COUNT = 20;
    static final int DEFAULT_BUCKET_COUNT = 1;
    static final int DEFAULT_DECISION_TASK_THREAD_COUNT = 20;
    static final int DEFAULT_PERIODIC_SUBMITTER_THREAD_COUNT = 1;

    private int activityTaskThreadCount = DEFAULT_ACTIVITY_TASK_THREAD_COUNT;
    private Integer activityTaskPollerThreadCount = null;
    private int bucketCount = DEFAULT_BUCKET_COUNT;
    private int decisionTaskThreadCount = DEFAULT_DECISION_TASK_THREAD_COUNT;
    private Integer decisionTaskPollerThreadCount;
    private int periodicSubmitterThreadCount = DEFAULT_PERIODIC_SUBMITTER_THREAD_COUNT;

    public int getActivityTaskThreadCount() {
        return activityTaskThreadCount;
    }

    /**
     * Any task lists that are referenced by workflows but not given a specific activity task thread count
     * will be given the default activity task thread count of DEFAULT_ACTIVITY_TASK_THREAD_COUNT.
     */
    public void setActivityTaskThreadCount(int count) {
        this.activityTaskThreadCount = count;
    }

    /**
     * See setActivityTaskPollerThreadCount.
     */
    public int getActivityTaskPollerThreadCount() {
        if (activityTaskPollerThreadCount == null) {
            return Math.max(1, activityTaskThreadCount / 2);
        }
        return activityTaskPollerThreadCount;
    }

    /**
     * Poller threads poll for work from SWF, then delegate the actual work off to the worker thread pool.
     *
     * Any task lists that are referenced by workflows but not given a specific poller thread count
     * will have a poller thread count equal to half of the corresponding activity task thread count.
     */
    public void setActivityTaskPollerThreadCount(int count) {
        this.activityTaskPollerThreadCount = count;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    /**
     * Workflows submitted to this task list are divided into this number of buckets, where each bucket is polled
     * separately. This should primarily be used to divide task lists with more than 200 events per second into
     * multiple buckets, each of which should stay below 200 events per second.
     *
     * The bucket count is applied to activity and decision task workers and pollers, but not periodic submitters.
     *
     * Keep in mind that while the bucket count can be safely increased for a task list (aside from throttling concerns),
     * lowering the bucket count can only be done safely if there are no workflows running for this task list, because
     * Flux only polls for work on task list buckets under this count.
     *
     * By default, the bucket count is 1.
     */
    public void setBucketCount(int bucketCount) {
        if (bucketCount < 1) {
            throw new IllegalArgumentException("bucketCount must be greater than or equal to 1.");
        }
        this.bucketCount = bucketCount;
    }

    public int getDecisionTaskThreadCount() {
        return decisionTaskThreadCount;
    }

    /**
     * Any task lists that are referenced by workflows but not given a specific decision task thread count
     * will be given the default decision task thread count of DEFAULT_DECISION_TASK_THREAD_COUNT.
     */
    public void setDecisionTaskThreadCount(int count) {
        this.decisionTaskThreadCount = count;
    }

    /**
     * See setDecisionTaskPollerThreadCount.
     */
    public int getDecisionTaskPollerThreadCount() {
        if (decisionTaskPollerThreadCount == null) {
            return Math.max(1, decisionTaskThreadCount / 2);
        }
        return decisionTaskPollerThreadCount;
    }

    /**
     * Poller threads poll for work from SWF, then delegate the actual work off to the decider thread pool.
     *
     * Any task lists that are referenced by workflows but not given a specific poller thread count
     * will have a poller thread count equal to half of the corresponding decision task thread count.
     */
    public void setDecisionTaskPollerThreadCount(int count) {
        this.decisionTaskPollerThreadCount = count;
    }

    public int getPeriodicSubmitterThreadCount() {
        return periodicSubmitterThreadCount;
    }

    /**
     * Any task lists that are referenced by periodic workflows but not given a specific submitter thread count
     * will be given the default periodic submitter thread count of DEFAULT_PERIODIC_SUBMITTER_THREAD_COUNT.
     */
    public void setPeriodicSubmitterThreadCount(int count) {
        this.periodicSubmitterThreadCount = count;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TaskListConfig that = (TaskListConfig) other;
        return bucketCount == that.bucketCount
               && activityTaskThreadCount == that.activityTaskThreadCount
               && decisionTaskThreadCount == that.decisionTaskThreadCount
               && periodicSubmitterThreadCount == that.periodicSubmitterThreadCount
               && Objects.equals(activityTaskPollerThreadCount, that.activityTaskPollerThreadCount)
               && Objects.equals(decisionTaskPollerThreadCount, that.decisionTaskPollerThreadCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketCount, activityTaskThreadCount, activityTaskPollerThreadCount, decisionTaskThreadCount,
                            decisionTaskPollerThreadCount, periodicSubmitterThreadCount);
    }
}
