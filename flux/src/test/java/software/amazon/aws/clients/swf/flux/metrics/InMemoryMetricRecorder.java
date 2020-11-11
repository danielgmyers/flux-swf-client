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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * For use in validating metrics emitted by methods under test.
 */
public class InMemoryMetricRecorder extends MetricRecorder {

    private final Map<String, String> properties;
    private final Map<String, Instant> dates;
    private final Map<String, Double> counts;
    private final Map<String, Duration> durations;

    public InMemoryMetricRecorder(String operation) {
        super(operation);
        this.properties = new HashMap<>();
        this.dates = new HashMap<>();
        this.counts = new HashMap<>();
        this.durations = new HashMap<>();
    }

    @Override
    protected void addPropertyHook(String name, String value) {
        properties.put(name, value);
    }

    @Override
    protected void addTimestampHook(String name, Instant time) {
        dates.put(name, time);
    }

    @Override
    protected void addCountHook(String name, double value) {
        if (!counts.containsKey(name)) {
            counts.put(name, value);
        } else {
            counts.put(name, counts.get(name) + value);
        }
    }

    @Override
    protected void addDurationHook(String name, Duration duration) {
        if (!durations.containsKey(name)) {
            durations.put(name, duration);
        } else {
            durations.put(name, durations.get(name).plus(duration));
        }
    }

    public Map<String, Double> getCounts() {
        return counts;
    }

    public Map<String, Duration> getDurations() {
        return durations;
    }

    public Map<String, Instant> getTimestamps() {
        return dates;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
