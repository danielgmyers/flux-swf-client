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

package com.danielgmyers.flux.clients.swf.spring;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.testutil.StubFluxCapacitor;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(locations = {"/test-context.xml"})
public class SpringInitializationTest {

    @Autowired
    FluxCapacitor fluxCapacitor;

    @Test
    public void testInitialization() {
        // The content of test-context.xml is technically part of this test.
        // We're verifying that FluxSpringCreator and FluxSpringInitializer, when used as described in the README,
        // produce a valid FluxCapacitor bean.
        Assertions.assertNotNull(fluxCapacitor);
        Assertions.assertInstanceOf(StubFluxCapacitor.class, fluxCapacitor);
    }
}
