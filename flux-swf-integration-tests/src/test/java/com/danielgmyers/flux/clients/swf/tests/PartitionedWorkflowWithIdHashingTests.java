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

package com.danielgmyers.flux.clients.swf.tests;

import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;

/**
 * This should run the same tests as PartitionedWorkflowTests, except it enables partition ID hashing.
 */
public class PartitionedWorkflowWithIdHashingTests extends PartitionedWorkflowTests {

    @Override
    protected void updateFluxCapacitorConfig(FluxCapacitorConfig config) {
        config.setEnablePartitionIdHashing(true);
    }
}
