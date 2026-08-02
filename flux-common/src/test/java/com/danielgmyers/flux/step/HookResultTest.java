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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HookResultTest {

    @Test
    public void proceed_isContinueAction() {
        HookResult result = HookResult.proceed();

        Assertions.assertTrue(result.isContinue());
        Assertions.assertFalse(result.isRetry());
        Assertions.assertFalse(result.isForceResult());
        Assertions.assertNull(result.getResultCode());
        Assertions.assertNull(result.getMessage());
    }

    @Test
    public void retryStep_noMessage_isRetryActionWithNullMessage() {
        HookResult result = HookResult.retryStep();

        Assertions.assertFalse(result.isContinue());
        Assertions.assertTrue(result.isRetry());
        Assertions.assertFalse(result.isForceResult());
        Assertions.assertNull(result.getResultCode());
        Assertions.assertNull(result.getMessage());
    }

    @Test
    public void retryStep_withMessage_isRetryActionWithMessage() {
        HookResult result = HookResult.retryStep("please retry");

        Assertions.assertFalse(result.isContinue());
        Assertions.assertTrue(result.isRetry());
        Assertions.assertFalse(result.isForceResult());
        Assertions.assertNull(result.getResultCode());
        Assertions.assertEquals("please retry", result.getMessage());
    }

    @Test
    public void overrideStepResult_codeOnly_isForceResultActionWithNullMessage() {
        HookResult result = HookResult.overrideStepResult("forced");

        Assertions.assertFalse(result.isContinue());
        Assertions.assertFalse(result.isRetry());
        Assertions.assertTrue(result.isForceResult());
        Assertions.assertEquals("forced", result.getResultCode());
        Assertions.assertNull(result.getMessage());
    }

    @Test
    public void overrideStepResult_codeAndMessage_isForceResultActionWithMessage() {
        HookResult result = HookResult.overrideStepResult("forced", "because reasons");

        Assertions.assertFalse(result.isContinue());
        Assertions.assertFalse(result.isRetry());
        Assertions.assertTrue(result.isForceResult());
        Assertions.assertEquals("forced", result.getResultCode());
        Assertions.assertEquals("because reasons", result.getMessage());
    }

    @Test
    public void overrideStepResult_nullCode_throwsNullPointerException() {
        Assertions.assertThrows(NullPointerException.class, () -> HookResult.overrideStepResult(null));
        Assertions.assertThrows(NullPointerException.class, () -> HookResult.overrideStepResult(null, "msg"));
    }
}
