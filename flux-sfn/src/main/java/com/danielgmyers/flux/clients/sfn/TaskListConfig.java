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

/**
 * Container for task-list-specific configuration data used by Flux at runtime.
 */
public class TaskListConfig {

    // Visible for testing
    static final int DEFAULT_ACTIVITY_TASK_THREAD_COUNT = 20;
    static final int DEFAULT_ACTIVITY_POLLER_THREAD_COUNT = 1;

    private int activityTaskThreadCount = DEFAULT_ACTIVITY_TASK_THREAD_COUNT;
    private int activityPollerThreadCount = DEFAULT_ACTIVITY_POLLER_THREAD_COUNT;

    public int getActivityTaskThreadCount() {
        return activityTaskThreadCount;
    }

    /**
     * Sets the number of threads available to execute activity tasks. This thread pool is shared by all activities across
     * all workflows that use this same task list.
     *
     * If no specific activity task thread count is provided, the default task thread count of DEFAULT_ACTIVITY_TASK_THREAD_COUNT
     * will be used.
     */
    public void setActivityTaskThreadCount(int count) {
        this.activityTaskThreadCount = count;
    }

    public int getActivityPollerThreadCount() {
        return activityPollerThreadCount;
    }

    /**
     * Sets the number of threads used to poll for work for each activity assigned to this task list.
     * For a Workflow with five WorkflowSteps, this will result in 5*count polling threads. Since polling is per activity,
     * it is recommended to keep this value small.
     *
     * If no specific activity poller thread count is provided, the default poller thread count of
     * DEFAULT_ACTIVITY_POLLER_THREAD_COUNT will be used.
     */
    public void setActivityPollerThreadCount(int count) {
        this.activityPollerThreadCount = count;
    }
}
