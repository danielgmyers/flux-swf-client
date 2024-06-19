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

package com.danielgmyers.flux.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArnUtilsTest {

    @Test
    public void testExtractQualifier_ValidArnWithQualifier() {
        Assertions.assertEquals("myworkflow", ArnUtils.extractQualifier("arn:aws:states:us-west-2:123456789012:execution:TestWorkflow:myworkflow"));
    }

    @Test
    public void testExtractQualifier_ValidArnWithoutQualifier() {
        Assertions.assertNull(ArnUtils.extractQualifier("arn:aws:states:us-west-2:123456789012:stateMachine:TestWorkflow"));
    }

    @Test
    public void testExtractQualifier_InvalidArn() {
        Assertions.assertNull(ArnUtils.extractQualifier("this:is:not:an:arn"));
    }
}
