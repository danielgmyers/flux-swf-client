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

package com.danielgmyers.flux.clients.swf.guice;

import java.util.List;

import com.danielgmyers.flux.clients.swf.guice.workflowone.WorkflowOne;
import com.danielgmyers.flux.clients.swf.guice.workflowtwo.WorkflowTwo;
import com.danielgmyers.flux.wf.Workflow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FluxModuleTest {

    @Test
    public void testWorkflowClassScanningFindsAllWorkflows() {
        FluxModule module = new FluxModule("com.danielgmyers.flux.clients.swf.guice");
        // we don't need to verify that everything's there, just that *something* is there
        List<Class<? extends Workflow>> classes = module.findWorkflowClassesFromClasspath();
        Assertions.assertEquals(2, classes.size());
        Assertions.assertTrue(classes.contains(WorkflowOne.class));
        Assertions.assertTrue(classes.contains(WorkflowTwo.class));
    }

    @Test
    public void testWorkflowClassScanningScopingExcludesWorkflowsOutsideScope() {
        FluxModule module = new FluxModule("com.danielgmyers.flux.clients.swf.guice.workflowone");
        // we don't need to verify that everything's there, just that *something* is there
        List<Class<? extends Workflow>> classes = module.findWorkflowClassesFromClasspath();
        Assertions.assertEquals(1, classes.size());
        Assertions.assertTrue(classes.contains(WorkflowOne.class));
        Assertions.assertFalse(classes.contains(WorkflowTwo.class));
    }
}
