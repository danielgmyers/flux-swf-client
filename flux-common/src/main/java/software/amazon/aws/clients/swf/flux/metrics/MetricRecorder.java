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

package software.amazon.aws.clients.swf.flux.metrics;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A generic interface for emitting metrics to an arbitrary recording mechanism.
 */
public abstract class MetricRecorder implements AutoCloseable {

    private boolean closed;
    private final Instant startTime;
    private final String operation;

    private final Map<String, Instant> timers = new HashMap<>();

    /**
     * Initializes the MetricRecorder.
     */
    protected MetricRecorder(String operation) {
        this.startTime = Instant.now();
        this.operation = operation;
    }

    /**
     * Closes this set of metrics. After close() is called, new metrics may no longer be recorded with this object.
     * Implementations should store, publish, or otherwise record the metrics at this time.
     *
     * This method closes any open durations, and emits the standard metrics listed in StandardMetricNames.
     */
    public final void close() {
        verifyNotClosed();

        Set<String> openTimers = new HashSet<>(timers.keySet());
        for (String openTimer : openTimers) {
            endDuration(openTimer);
        }

        addProperty(StandardMetricNames.OPERATION.toString(), operation);
        addProperty(StandardMetricNames.THREAD_NAME.toString(), Thread.currentThread().getName());
        addTimestamp(StandardMetricNames.START_TIME.toString(), startTime);

        Instant endTime = Instant.now();
        Duration time = Duration.between(startTime, endTime);
        addTimestamp(StandardMetricNames.END_TIME.toString(), endTime);
        addDuration(StandardMetricNames.TIME.toString(), time);

        closeHook();

        closed = true;
    }

    /**
     * Indicates whether the MetricRecorder is closed. If closed, it will reject recording more metrics.
     */
    public final boolean isClosed() {
        return closed;
    }

    /**
     * Records an arbitrary String value alongside the other metrics. This is useful to label this group of metrics.
     * If multiple properties are recorded with the same name, implementations *may* choose to throw an exception,
     * but *must* only honor one of them.
     */
    public final void addProperty(String name, String value) {
        verifyNotClosed();
        addPropertyHook(name, value);
    }

    /**
     * Records a timestamp.
     * If multiple timestamps are recorded with the same name, implementations *may* choose to throw an exception,
     * but *must* only honor one of them.
     */
    public final void addTimestamp(String name, Instant time) {
        verifyNotClosed();
        addTimestampHook(name, time);
    }

    /**
     * Records a count metric using a specific unit.
     * If multiple counts are recorded with the same name, implementations may choose to aggregate them.
     */
    public final void addCount(String name, double count) {
        verifyNotClosed();
        addCountHook(name, count);
    }

    /**
     * Records a specific duration metric.
     * If multiple durations are recorded with the same name, implementations may choose to aggregate them.
     */
    public final void addDuration(String name, Duration duration) {
        verifyNotClosed();
        addDurationHook(name, duration);
    }

    /**
     * Records the current time as reported by Instant.now() as a timestamp with the specified name.
     * Call endDuration() with the same name or close() to record the duration.
     *
     * Returns the stored start time.
     */
    public final Instant startDuration(String name) {
        return startDuration(name, Instant.now());
    }

    /**
     * Records the current time as reported by the provided Clock as a timestamp with the specified name.
     * Call endDuration() with the same name or close() to record the duration.
     *
     * Returns the stored start time.
     */
    public final Instant startDuration(String name, Clock clock) {
        return startDuration(name, clock.instant());
    }

    /**
     * Records the specified start time as a timestamp with the specified name.
     * Call endDuration() with the same name or close() to record the duration.
     *
     * Returns the stored start time.
     */
    public final Instant startDuration(String name, Instant startTime) {
        verifyNotClosed();
        if (timers.containsKey(name)) {
            throw new IllegalStateException("A timer named " + name + " is already open.");
        }
        timers.put(name, startTime);
        return startTime;
    }

    /**
     * When startDuration() was called with the same name, this method calls
     * addDuration() using the duration between the recorded start time
     * and the current time as reported by Instant.now().
     *
     * Returns the measured duration.
     */
    public final Duration endDuration(String name) {
        return endDuration(name, Instant.now());
    }

    /**
     * When startDuration() was called with the same name, this method calls
     * addDuration() using the duration between the recorded start time
     * and the current time as reported by the provided Clock.
     *
     * Returns the measured duration.
     */
    public final Duration endDuration(String name, Clock clock) {
        return endDuration(name, clock.instant());
    }

    /**
     * When startDuration() was called with the same name, this method calls
     * addDuration() using the duration between the recorded start time
     * and the specified end time.
     *
     * Returns the measured duration.
     */
    public final Duration endDuration(String name, Instant endTime) {
        verifyNotClosed();
        if (!timers.containsKey(name)) {
            throw new IllegalStateException("No active timer named " + name);
        }
        Instant startTime = timers.remove(name);
        Duration duration = Duration.between(startTime, endTime);
        addDuration(name, duration);
        return duration;
    }

    /**
     * Hook method for implementation-specific behavior.
     *
     * Default is to do nothing.
     */
    protected void closeHook() {}

    /**
     * Hook method for implementation-specific behavior.
     *
     * Default is to do nothing.
     */
    protected void addPropertyHook(String name, String value) {}

    /**
     * Hook method for implementation-specific behavior.
     *
     * Default is to do nothing.
     */
    protected void addTimestampHook(String name, Instant time) {}

    /**
     * Hook method for implementation-specific behavior.
     *
     * Default is to do nothing.
     */
    protected void addCountHook(String name, double count) {}

    /**
     * Hook method for implementation-specific behavior.
     *
     * Default is to do nothing.
     */
    protected void addDurationHook(String name, Duration duration) {}

    protected final String getOperation() {
        return operation;
    }

    private void verifyNotClosed() {
        if (closed) {
            throw new IllegalStateException("MetricRecorder is already closed.");
        }
    }
}
