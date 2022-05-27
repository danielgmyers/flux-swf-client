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

package software.amazon.aws.clients.swf.flux.metrics;

/**
 * Produces MetricRecorder objects that discard all metrics.
 */
public class NoopMetricRecorderFactory implements MetricRecorderFactory {
    @Override
    public MetricRecorder newMetricRecorder(String operation) {
        // The default MetricRecorder hooks do nothing, so the base class itself is already a noop recorder.
        // We can't use a singleton instance because the disallow-writes-after-close behavior needs to be enforced.
        return new MetricRecorder(operation) {};
    }
}
