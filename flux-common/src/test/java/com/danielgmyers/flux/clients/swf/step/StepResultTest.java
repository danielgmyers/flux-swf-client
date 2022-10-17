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

package com.danielgmyers.flux.clients.swf.step;

import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.step.StepResult.ResultAction;
import org.junit.Assert;
import org.junit.Test;

public class StepResultTest {

    @Test
    public void testSuccessHasRightAction() {
        Assert.assertEquals(ResultAction.COMPLETE, StepResult.success().getAction());
        Assert.assertEquals(ResultAction.COMPLETE, StepResult.success("message").getAction());
    }

    @Test
    public void testRetryHasRightAction() {
        Assert.assertEquals(ResultAction.RETRY, StepResult.retry().getAction());
        Assert.assertEquals(ResultAction.RETRY, StepResult.retry("message").getAction());
    }

    @Test
    public void testFailureHasRightAction() {
        Assert.assertEquals(ResultAction.COMPLETE, StepResult.failure().getAction());
        Assert.assertEquals(ResultAction.COMPLETE, StepResult.failure("message").getAction());
    }

    @Test
    public void testCompleteHasRightAction() {
        Assert.assertEquals(ResultAction.COMPLETE, StepResult.complete("customResultCode", "message").getAction());
    }

    @Test
    public void testDisallowRetryWithResultCode() {
        try {
            new StepResult(ResultAction.RETRY, "something", "message");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowNullResultCodeForSuccess() {
        try {
            StepResult.complete(null, "message");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowBlankResultCodeForSuccess() {
        try {
            StepResult.complete("", "message");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowNullResultCodeForFailure() {
        try {
            StepResult.complete(null, "message");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDisallowBlankResultCodeForFailure() {
        try {
            StepResult.complete("", "message");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testAddAttribute() {
        StepResult r = StepResult.success();
        Assert.assertTrue(r.getAttributes().isEmpty());
        r.addAttribute("foo", "bar");
        Assert.assertFalse(r.getAttributes().isEmpty());
        Assert.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void testWithAttribute() {
        StepResult r = StepResult.success();
        Assert.assertTrue(r.getAttributes().isEmpty());
        Assert.assertEquals(r, r.withAttribute("foo", "bar"));
        Assert.assertFalse(r.getAttributes().isEmpty());
        Assert.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void testWithAttributes() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        StepResult r = StepResult.success();
        Assert.assertTrue(r.getAttributes().isEmpty());
        Assert.assertEquals(r, r.withAttributes(input));
        Assert.assertFalse(r.getAttributes().isEmpty());
        Assert.assertEquals("bar", r.getAttributes().get("foo"));
    }

    @Test
    public void disallowAddAttributeForRetry() {
        StepResult r = StepResult.retry();
        Assert.assertTrue(r.getAttributes().isEmpty());
        try {
            r.addAttribute("foo", "bar");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assert.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void disallowWithAttributeForRetry() {
        StepResult r = StepResult.retry();
        Assert.assertTrue(r.getAttributes().isEmpty());
        try {
            r.withAttribute("foo", "bar");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assert.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void disallowWithAttributesForRetry() {
        Map<String, String> input = new HashMap<>();
        input.put("foo", "bar");

        StepResult r = StepResult.retry();
        Assert.assertTrue(r.getAttributes().isEmpty());
        try {
            r.withAttributes(input);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        Assert.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void allowWithAttributesForRetryIfMapNull() {
        StepResult r = StepResult.retry().withAttributes(null);
        Assert.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void allowWithAttributesForRetryIfMapEmpty() {
        Map<String, String> input = new HashMap<>();

        StepResult r = StepResult.retry().withAttributes(input);
        Assert.assertTrue(r.getAttributes().isEmpty());
    }

    @Test
    public void testEquals() {
        Assert.assertEquals(StepResult.success(), StepResult.success());
        Assert.assertNotEquals(StepResult.success(), StepResult.failure());
        Assert.assertNotEquals(StepResult.success(), StepResult.retry());

        Assert.assertEquals(StepResult.failure(), StepResult.failure());
        Assert.assertNotEquals(StepResult.failure(), StepResult.success());
        Assert.assertNotEquals(StepResult.failure(), StepResult.retry());

        Assert.assertEquals(StepResult.retry(), StepResult.retry());
        Assert.assertNotEquals(StepResult.retry(), StepResult.success());
        Assert.assertNotEquals(StepResult.retry(), StepResult.failure());

        // only difference here is the message
        Assert.assertNotEquals(StepResult.success("some message"), StepResult.success("another message"));

        // only difference here is the result code
        Assert.assertNotEquals(StepResult.complete("customResult", "some message"), StepResult.success("some message"));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(StepResult.success().hashCode(), StepResult.success().hashCode());
        Assert.assertNotEquals(StepResult.success().hashCode(), StepResult.failure().hashCode());
        Assert.assertNotEquals(StepResult.success().hashCode(), StepResult.retry().hashCode());

        Assert.assertEquals(StepResult.failure().hashCode(), StepResult.failure().hashCode());
        Assert.assertNotEquals(StepResult.failure().hashCode(), StepResult.success().hashCode());
        Assert.assertNotEquals(StepResult.failure().hashCode(), StepResult.retry().hashCode());

        Assert.assertEquals(StepResult.retry().hashCode(), StepResult.retry().hashCode());
        Assert.assertNotEquals(StepResult.retry().hashCode(), StepResult.success().hashCode());
        Assert.assertNotEquals(StepResult.retry().hashCode(), StepResult.failure().hashCode());

        // only difference here is the message
        Assert.assertNotEquals(StepResult.success("some message").hashCode(), StepResult.success("another message").hashCode());

        // only difference here is the result code
        Assert.assertNotEquals(StepResult.complete("customResult", "some message").hashCode(),
                               StepResult.success("some message").hashCode());
    }
}
