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

package com.danielgmyers.flux.clients.swf.metrics;

/**
 * An enum representing a few standard metric names. Metrics with these names will be emitted by all MetricRecorder
 * implementations. When the MetricRecorder's close() method returns, the following metrics will have been recorded:
 *
 * The "Operation" property will contain the value that was passed to the MetricRecorderFactory.newMetricRecorder() method.
 * The "ThreadName" property will contain the name of the current thread at the time the MetricRecorder is closed.
 *
 * The StartTime timestamp will contain the Instant at which the MetricRecorder object was created.
 * The EndTime timestamp will contain the Instant at which the MetricRecorder object's close() method was called.
 *
 * The Time duration will contain the duration between the StartTime and EndTime timestamps.
 */
public enum StandardMetricNames {
    // Properties
    OPERATION("Operation"),
    THREAD_NAME("ThreadName"),

    // Timestamps
    START_TIME("StartTime"),
    END_TIME("EndTime"),

    // Durations
    TIME("Time"),

    // Counts - none for now
    ;

    private final String name;

    StandardMetricNames(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
