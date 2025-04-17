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

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.metrics.MetricRecorderFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Helper providing a factory method for building a FluxCapacitor.
 */
public final class FluxCapacitorFactory {

    private FluxCapacitorFactory() {}

    /**
     * Creates a FluxCapacitor object.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param credentials    - A provider for the AWS credentials that should be used to call AWS APIs
     * @param config         - A FluxCapacitorConfig object containing the relevant configuration information for Flux
     */
    public static FluxCapacitor create(MetricRecorderFactory metricsFactory, AwsCredentialsProvider credentials,
                                       FluxCapacitorConfig config) {
        return FluxCapacitorImpl.create(metricsFactory, credentials, config);
    }
}
