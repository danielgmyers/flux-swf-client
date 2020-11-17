package software.amazon.aws.clients.swf.flux.guice;

import java.util.Map;

import com.google.inject.Inject;

/**
 * For internal use only - allows certain configurations to be optionally provided by users,
 * with default behavior if they're not provided.
 */
public class FluxOptionalConfigHolder {

    private Double exponentialBackoffBase = null;
    private String swfEndpoint = null;
    private Map<String, Integer> taskListActivityThreadCounts = null;
    private Map<String, Integer> taskListActivityPollerThreadCounts = null;
    private Map<String, Integer> taskListDeciderThreadCounts = null;
    private Map<String, Integer> taskListDeciderPollerThreadCounts = null;
    private Map<String, Integer> taskListPeriodicSubmitterThreadCounts = null;

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

    public Map<String, Integer> getTaskListActivityThreadCounts() {
        return taskListActivityThreadCounts;
    }

    @Inject(optional = true)
    public void setTaskListActivityThreadCounts(@TaskListActivityThreadCounts
                                                        Map<String, Integer> taskListActivityThreadCounts) {
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
