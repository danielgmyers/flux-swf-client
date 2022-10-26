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

package com.danielgmyers.flux.step;

import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.step.StepResult.ResultAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StepResultTest {

    @Test
    public void testSuccessHasRightAction() {
        Assertions.assertEquals(ResultAction.COMPLETE, StepResult.success().getAction());
        Assertions.assertEquals(ResultAction.COMPLETE, StepResult.success("message").getAction());
    }

    @Test
    public void testRetryHasRightAction() {
        Assertions.assertEquals(ResultAction.RETRY, StepResult.retry().getAction());
        Assertions.assertEquals(ResultAction.RETRY, StepResult.retry("message").getAction());
    }

    @Test
    public void testFailureHasRightAction() {
        Assertions.assertEquals(ResultAction.COMPLETE, StepResult.failure().getAction());
        Assertions.assertEquals(ResultAction.COMPLETE, StepResult.failure("message").getAction());
    }

    @Test
    public void testCompleteHasRightAction() {
        Assertions.assertEquals(ResultAction.COMPLETE, StepResult.complete("customResultCode", "message").getAction());
    }

    @Test
    public void testDisallowRetryWithResultCode() {
        try {
            new StepResult(ResultAction.RETRY, "something", "message");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowNullResultCodeForSuccess() {
        try {
            StepResult.complete(null, "message");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowBlankResultCodeForSuccess() {
        try {
            StepResult.complete("", "message");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowNullResultCodeForFailure() {
        try {
            StepResult.complete(null, "message");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowBlankResultCodeForFailure() {
        try {
            StepResult.complete("", "message");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testAddAttribute() {
        StepResult r = StepResult.success();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        r.addAttribute("foo", "bar");
        Assertions.assertFalse(r.getAttributes().isEmpty());
        Assertions.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void testWithAttribute() {
        StepResult r = StepResult.success();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        Assertions.assertEquals(r, r.withAttribute("foo", "bar"));
        Assertions.assertFalse(r.getAttributes().isEmpty());
        Assertions.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void testWithAttributes() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        StepResult r = StepResult.success();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        Assertions.assertEquals(r, r.withAttributes(input));
        Assertions.assertFalse(r.getAttributes().isEmpty());
        Assertions.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void disallowAddAttributeForRetry() {
        StepResult r = StepResult.retry();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        try {
            r.addAttribute("foo", "bar");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assertions.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void disallowWithAttributeForRetry() {
        StepResult r = StepResult.retry();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        try {
            r.withAttribute("foo", "bar");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assertions.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void disallowWithAttributesForRetry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        StepResult r = StepResult.retry();
        Assertions.assertTrue(r.getAttributes().isEmpty());
        try {
            r.withAttributes(input);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assertions.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void allowWithAttributesForRetryIfMapNull() {
        StepResult r = StepResult.retry().withAttributes(null);
        Assertions.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void allowWithAttributesForRetryIfMapEmpty() {
        Map<String, String> input = new HashMap<>();

        StepResult r = StepResult.retry().withAttributes(input);
        Assertions.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void testEquals() {
        Assertions.assertEquals(StepResult.success(), StepResult.success());
        Assertions.assertNotEquals(StepResult.success(), StepResult.failure());
        Assertions.assertNotEquals(StepResult.success(), StepResult.retry());

        Assertions.assertEquals(StepResult.failure(), StepResult.failure());
        Assertions.assertNotEquals(StepResult.failure(), StepResult.success());
        Assertions.assertNotEquals(StepResult.failure(), StepResult.retry());

        Assertions.assertEquals(StepResult.retry(), StepResult.retry());
        Assertions.assertNotEquals(StepResult.retry(), StepResult.success());
        Assertions.assertNotEquals(StepResult.retry(), StepResult.failure());

        // only difference here is the message
        Assertions.assertNotEquals(StepResult.success("some message"), StepResult.success("another message"));

        // only difference here is the result code
        Assertions.assertNotEquals(StepResult.complete("customResult", "some message"), StepResult.success("some message"));
    }

    @Test
    public void testHashCode() {
        Assertions.assertEquals(StepResult.success().hashCode(), StepResult.success().hashCode());
        Assertions.assertNotEquals(StepResult.success().hashCode(), StepResult.failure().hashCode());
        Assertions.assertNotEquals(StepResult.success().hashCode(), StepResult.retry().hashCode());

        Assertions.assertEquals(StepResult.failure().hashCode(), StepResult.failure().hashCode());
        Assertions.assertNotEquals(StepResult.failure().hashCode(), StepResult.success().hashCode());
        Assertions.assertNotEquals(StepResult.failure().hashCode(), StepResult.retry().hashCode());

        Assertions.assertEquals(StepResult.retry().hashCode(), StepResult.retry().hashCode());
        Assertions.assertNotEquals(StepResult.retry().hashCode(), StepResult.success().hashCode());
        Assertions.assertNotEquals(StepResult.retry().hashCode(), StepResult.failure().hashCode());

        // only difference here is the message
        Assertions.assertNotEquals(StepResult.success("some message").hashCode(), StepResult.success("another message").hashCode());

        // only difference here is the result code
        Assertions.assertNotEquals(StepResult.complete("customResult", "some message").hashCode(),
                               StepResult.success("some message").hashCode());
    }
}
