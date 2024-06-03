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

package com.danielgmyers.flux.clients.swf.poller;

import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutionContextMetadataTest {

    public static class EatSandwich implements WorkflowStep {
        @StepApply
        public void doThing() {
        }
    }

    public static class DrinkWater implements WorkflowStep {
        @StepApply
        public void doThing() {
        }
    }

    public static class ThrowAwayBadSandwich implements WorkflowStep {
        @StepApply
        public void doThing() {
        }
    }

    public static class TestWorkflow implements Workflow {
        @Override
        public WorkflowGraph getGraph() {
            EatSandwich eat = new EatSandwich();
            DrinkWater dw = new DrinkWater();
            ThrowAwayBadSandwich tabs = new ThrowAwayBadSandwich();

            WorkflowGraphBuilder builder = new WorkflowGraphBuilder(eat);
            builder.successTransition(eat, dw);
            builder.failTransition(eat, tabs);

            builder.addStep(dw);
            builder.alwaysClose(dw);

            builder.addStep(tabs);
            builder.alwaysClose(tabs);

            return builder.build();
        }
    }

    private static final String DELAY_EXIT_CONTEXT
            = "{\"" + ExecutionContextMetadata.KEY_METADATA_VERSION + "\":" + ExecutionContextMetadata.CURRENT_METADATA_VERSION + "," +
            "\"" + ExecutionContextMetadata.KEY_NEXT_STEP_NAME + "\":\"" + DecisionTaskPoller.DELAY_EXIT_TIMER_ID + "\"}";

    private static final String EAT_SANDWICH_EXPECTED_CONTEXT
            = "{\"" + ExecutionContextMetadata.KEY_METADATA_VERSION + "\":" + ExecutionContextMetadata.CURRENT_METADATA_VERSION + "," +
               "\"" + ExecutionContextMetadata.KEY_NEXT_STEP_NAME + "\":\"" + EatSandwich.class.getSimpleName() + "\"," +
               "\"" + ExecutionContextMetadata.KEY_NEXT_STEP_RESULT_CODES + "\":{" +
                  "\"" + StepResult.SUCCEED_RESULT_CODE + "\":\"" + DrinkWater.class.getSimpleName() + "\"," +
                  "\"" + StepResult.FAIL_RESULT_CODE + "\":\"" + ThrowAwayBadSandwich.class.getSimpleName() + "\"" +
              "}}";

    private static final String DRINK_WATER_EXPECTED_CONTEXT
            = "{\"" + ExecutionContextMetadata.KEY_METADATA_VERSION + "\":" + ExecutionContextMetadata.CURRENT_METADATA_VERSION + "," +
               "\"" + ExecutionContextMetadata.KEY_NEXT_STEP_NAME + "\":\"" + DrinkWater.class.getSimpleName() + "\"," +
               "\"" + ExecutionContextMetadata.KEY_NEXT_STEP_RESULT_CODES + "\":{" +
                  "\"" + StepResult.ALWAYS_RESULT_CODE + "\":\"" + ExecutionContextMetadata.NEXT_STEP_RESULT_WORKFLOW_ENDS + "\"" +
            "}}";

    @Test
    public void testEncodeEmptyContext() throws JsonProcessingException {
        ExecutionContextMetadata ecm = new ExecutionContextMetadata();
        Assertions.assertNull(ecm.encode());
    }

    @Test
    public void testEncodeContextWithOnlyNextStepName() throws JsonProcessingException {
        ExecutionContextMetadata ecm = new ExecutionContextMetadata();
        ecm.setNextStepName(DecisionTaskPoller.DELAY_EXIT_TIMER_ID);
        Assertions.assertEquals(DELAY_EXIT_CONTEXT, ecm.encode());
    }

    @Test
    public void testEncodeContextWithResultCodeMap() throws JsonProcessingException {
        TestWorkflow wf = new TestWorkflow();

        ExecutionContextMetadata ecm = new ExecutionContextMetadata();
        ecm.populateExecutionContext(EatSandwich.class, wf.getGraph());

        Assertions.assertEquals(EAT_SANDWICH_EXPECTED_CONTEXT, ecm.encode());
    }

    @Test
    public void testEncodeContextWithResultCodeMapWhenWorkflowEnds() throws JsonProcessingException {
        TestWorkflow wf = new TestWorkflow();

        ExecutionContextMetadata ecm = new ExecutionContextMetadata();
        ecm.populateExecutionContext(DrinkWater.class, wf.getGraph());

        Assertions.assertEquals(DRINK_WATER_EXPECTED_CONTEXT, ecm.encode());
    }

    @Test
    public void testDecodeNullContext() throws JsonProcessingException {
        Assertions.assertNull(ExecutionContextMetadata.decode(null));
    }

    @Test
    public void testDecodeContextWithOnlyNextStepName() throws JsonProcessingException {
        ExecutionContextMetadata ecm = ExecutionContextMetadata.decode(DELAY_EXIT_CONTEXT);
        Assertions.assertNotNull(ecm);
        Assertions.assertEquals(ExecutionContextMetadata.CURRENT_METADATA_VERSION, ecm.getMetadataVersion());
        Assertions.assertEquals(DecisionTaskPoller.DELAY_EXIT_TIMER_ID, ecm.getNextStepName());
        Assertions.assertNull(ecm.getResultCodeMap());
    }

    @Test
    public void testDecodeContextWithResultCodeMap() throws JsonProcessingException {
        ExecutionContextMetadata ecm = ExecutionContextMetadata.decode(EAT_SANDWICH_EXPECTED_CONTEXT);
        Assertions.assertNotNull(ecm);
        Assertions.assertEquals(ExecutionContextMetadata.CURRENT_METADATA_VERSION, ecm.getMetadataVersion());
        Assertions.assertEquals(EatSandwich.class.getSimpleName(), ecm.getNextStepName());
        Assertions.assertNotNull(ecm.getResultCodeMap());
        Assertions.assertEquals(2, ecm.getResultCodeMap().size());
        Assertions.assertEquals(DrinkWater.class.getSimpleName(),
                                ecm.getResultCodeMap().get(StepResult.SUCCEED_RESULT_CODE));
        Assertions.assertEquals(ThrowAwayBadSandwich.class.getSimpleName(),
                                ecm.getResultCodeMap().get(StepResult.FAIL_RESULT_CODE));
    }

    @Test
    public void testDecodeContextWithResultCodeMapWhenWorkflowEnds() throws JsonProcessingException {
        ExecutionContextMetadata ecm = ExecutionContextMetadata.decode(DRINK_WATER_EXPECTED_CONTEXT);
        Assertions.assertNotNull(ecm);
        Assertions.assertEquals(ExecutionContextMetadata.CURRENT_METADATA_VERSION, ecm.getMetadataVersion());
        Assertions.assertEquals(DrinkWater.class.getSimpleName(), ecm.getNextStepName());
        Assertions.assertNotNull(ecm.getResultCodeMap());
        Assertions.assertEquals(1, ecm.getResultCodeMap().size());
        Assertions.assertEquals(ExecutionContextMetadata.NEXT_STEP_RESULT_WORKFLOW_ENDS,
                                ecm.getResultCodeMap().get(StepResult.ALWAYS_RESULT_CODE));
    }
}
