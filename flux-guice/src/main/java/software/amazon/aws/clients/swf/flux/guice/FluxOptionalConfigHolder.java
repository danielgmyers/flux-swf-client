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

package software.amazon.aws.clients.swf.flux.guice;

import java.util.Map;

import com.google.inject.Inject;

/**
 * For internal use only - allows certain configurations to be optionally provided by users,
 * with default behavior if they're not provided.
 */
public class FluxOptionalConfigHolder {

    private Boolean automaticallyTagExecutionsWithTaskList = null;
    private Double exponentialBackoffBase = null;
    private String swfEndpoint = null;
    private Map<String, Integer> taskListBucketCounts = null;
    private Map<String, Integer> taskListActivityThreadCounts = null;
    private Map<String, Integer> taskListActivityPollerThreadCounts = null;
    private Map<String, Integer> taskListDeciderThreadCounts = null;
    private Map<String, Integer> taskListDeciderPollerThreadCounts = null;
    private Map<String, Integer> taskListPeriodicSubmitterThreadCounts = null;

    public Boolean getAutomaticallyTagExecutionsWithTaskList() {
        return automaticallyTagExecutionsWithTaskList;
    }

    @Inject(optional = true)
    public void setAutomaticallyTagExecutionsWithTaskList(
            @AutomaticallyTagExecutionsWithTaskList Boolean automaticallyTagExecutionsWithTaskList) {
        this.automaticallyTagExecutionsWithTaskList = automaticallyTagExecutionsWithTaskList;
    }

    public Double getExponentialBackoffBase() {
        return exponentialBackoffBase;
    }

    @Inject(optional = true)
    public void setExponentialBackoffBase(@ExponentialBackoffBase Double exponentialBackoffBase) {
        this.exponentialBackoffBase = exponentialBackoffBase;
    }

    public String getSwfEndpoint() {
        return swfEndpoint;
    }

    @Inject(optional = true)
    public void setSwfEndpoint(@SwfEndpoint String swfEndpoint) {
        this.swfEndpoint = swfEndpoint;
    }

    public Map<String, Integer> getTaskListBucketCounts() {
        return taskListBucketCounts;
    }

    @Inject(optional = true)
    public void setTaskListBucketCounts(@TaskListBucketCounts Map<String, Integer> taskListBucketCounts) {
        this.taskListBucketCounts = taskListBucketCounts;
    }

    public Map<String, Integer> getTaskListActivityThreadCounts() {
        return taskListActivityThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListActivityThreadCounts(@TaskListActivityThreadCounts Map<String, Integer> taskListActivityThreadCounts) {
        this.taskListActivityThreadCounts = taskListActivityThreadCounts;
    }

    public Map<String, Integer> getTaskListActivityPollerThreadCounts() {
        return taskListActivityPollerThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListActivityPollerThreadCounts(@TaskListActivityPollerThreadCounts
                                                      Map<String, Integer> taskListActivityPollerThreadCounts) {
        this.taskListActivityPollerThreadCounts = taskListActivityPollerThreadCounts;
    }

    public Map<String, Integer> getTaskListDeciderThreadCounts() {
        return taskListDeciderThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListDeciderThreadCounts(@TaskListDeciderThreadCounts
                                               Map<String, Integer> taskListDeciderThreadCounts) {
        this.taskListDeciderThreadCounts = taskListDeciderThreadCounts;
    }

    public Map<String, Integer> getTaskListDeciderPollerThreadCounts() {
        return taskListDeciderPollerThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListDeciderPollerThreadCounts(@TaskListDeciderPollerThreadCounts
                                                     Map<String, Integer> taskListDeciderPollerThreadCounts) {
        this.taskListDeciderPollerThreadCounts = taskListDeciderPollerThreadCounts;
    }

    public Map<String, Integer> getTaskListPeriodicSubmitterThreadCounts() {
        return taskListPeriodicSubmitterThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListPeriodicSubmitterThreadCounts(@TaskListPeriodicSubmitterThreadCounts
                                                         Map<String, Integer> taskListPeriodicSubmitterThreadCounts) {
        this.taskListPeriodicSubmitterThreadCounts = taskListPeriodicSubmitterThreadCounts;
    }
}
