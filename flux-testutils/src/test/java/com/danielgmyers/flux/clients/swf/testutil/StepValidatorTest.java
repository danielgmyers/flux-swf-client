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

package com.danielgmyers.flux.clients.swf.testutil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepResult;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import org.junit.Assert;
import org.junit.Test;

public class StepValidatorTest {

    @Test
    public void testApplySucceeds_HappyCase() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.success();
        stub.setApplyResult(expected);

        Assert.assertEquals(expected, StepValidator.succeeds(stub, input));
    }

    @Test
    public void testApplySucceeds_ThrowsIfStepRetries() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.retry();
        stub.setApplyResult(expected);

        try {
            StepValidator.succeeds(stub, input);
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testApplySucceeds_ThrowsIfStepRetriesViaException_StepValidatorExceptionIncludesNestedException() {
        Throwable t = new IOException("testing");
        ThrowsExceptionStep stub = new ThrowsExceptionStep(t);
        Map<String, Object> input = new HashMap<>();

        try {
            StepValidator.succeeds(stub, input);
            Assert.fail();
        } catch(RuntimeException e) {
            Assert.assertEquals(t, e.getCause());
        }
    }

    @Test
    public void testApplySucceeds_ThrowsIfStepFails() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.failure();
        stub.setApplyResult(expected);

        try {
            StepValidator.succeeds(stub, input);
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testApplyRetries_HappyCase() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.retry();
        stub.setApplyResult(expected);

        Assert.assertEquals(expected, StepValidator.retries(stub, input));
    }

    @Test
    public void testApplyRetries_ThrowsIfStepSucceeds() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.success();
        stub.setApplyResult(expected);

        try {
            StepValidator.retries(stub, input);
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testApplyRetries_ThrowsIfStepFails() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.failure();
        stub.setApplyResult(expected);

        try {
            StepValidator.retries(stub, input);
            Assert.fail();
        } catch(RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testApplyFails_HappyCase() {
        StubStep stub = new StubStep();
        Map<String, Object> input = new HashMap<>();

        StepResult expected = StepResult.failure();
        stub.setApplyResult(expected);

        Assert.assertEquals(expected, StepValidator.fails(stub, input));
    }

    public static class StubStep implements WorkflowStep {

        private StepResult applyResult = null;

        public void setApplyResult(StepResult result) {
            this.applyResult = result;
        }

        @StepApply
        public StepResult apply() {
            return applyResult;
        }
    }

    public static class ThrowsExceptionStep implements WorkflowStep {

        private final Throwable t;

        public ThrowsExceptionStep(Throwable t) {
            this.t = t;
        }

        @StepApply
        public StepResult apply() throws Throwable {
            throw t;
        }
    }
}
