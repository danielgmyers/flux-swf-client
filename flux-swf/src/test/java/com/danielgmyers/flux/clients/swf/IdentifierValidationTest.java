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

package com.danielgmyers.flux.clients.swf;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IdentifierValidationTest {

    private static String invalidCharacters;

    @BeforeAll
    public static void setup() {
        StringBuilder sb = new StringBuilder();
        for (char c = '\u0000'; c <= '\u001f'; c++) {
            sb.append(c);
        }
        for (char c = '\u007f'; c <= '\u009f'; c++) {
            sb.append(c);
        }
        sb.append(':');
        sb.append('/');
        sb.append('|');
        invalidCharacters = sb.toString();
    }

    @Test
    public void testDomain() {
        doCommonTests(IdentifierValidation::validateDomain, IdentifierValidation.MAX_DOMAIN_NAME_LENGTH, false);
        Assertions.assertThrows(IllegalArgumentException.class, () -> IdentifierValidation.validateDomain("arn"));
    }

    @Test
    public void testTaskListName() {
        doCommonTests(IdentifierValidation::validateTaskListName, IdentifierValidation.MAX_TASK_LIST_NAME_LENGTH, false);
        Assertions.assertThrows(IllegalArgumentException.class, () -> IdentifierValidation.validateTaskListName("arn"));
    }

    @Test
    public void testWorkflowExecutionId() {
        doCommonTests(IdentifierValidation::validateWorkflowExecutionId, IdentifierValidation.MAX_WORKFLOW_EXECUTION_ID_LENGTH, false);
        Assertions.assertThrows(IllegalArgumentException.class, () -> IdentifierValidation.validateWorkflowExecutionId("arn"));
    }

    @Test
    public void testHostname() {
        doCommonTests(IdentifierValidation::validateHostname, IdentifierValidation.MAX_HOSTNAME_LENGTH, true);
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateHostname("arn"));
    }

    @Test
    public void testWorkflowExecutionTag() {
        doCommonTests(IdentifierValidation::validateWorkflowExecutionTag, IdentifierValidation.MAX_WORKFLOW_EXECUTION_TAG_LENGTH, true);
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateWorkflowExecutionTag("arn"));
    }

    @Test
    public void testPartitionId() {
        doCommonTests((s) -> IdentifierValidation.validatePartitionId(s, true), IdentifierValidation.MAX_PARTITION_ID_LENGTH_HASHING_ENABLED, true);
        doCommonTests((s) -> IdentifierValidation.validatePartitionId(s, false), IdentifierValidation.MAX_PARTITION_ID_LENGTH_HASHING_DISABLED, false);

        // "arn" should be allowed either way, even though invalid characters aren't allowed with hashing disabled.
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validatePartitionId("arn", true));
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validatePartitionId("arn", false));
    }

    public static class TestWorkflowWithValidClassName implements Workflow {
        // implementation doesn't need to be valid for this test
        @Override
        public WorkflowGraph getGraph() {
            return null;
        }
    }

    public static class TestWorkflowWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInRealCodeNoMatterHowWideYourMonitorIs implements Workflow {
        // implementation doesn't need to be valid for this test
        @Override
        public WorkflowGraph getGraph() {
            return null;
        }
    }

    public static class TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely implements Workflow {
        // implementation doesn't need to be valid for this test
        @Override
        public WorkflowGraph getGraph() {
            return null;
        }
    }

    @Test
    public void testWorkflowClassName() {
        // these two checks just ensure the test will break if the max name length constant or the class names get changed.
        Assertions.assertEquals(IdentifierValidation.MAX_WORKFLOW_CLASS_NAME_LENGTH, TestWorkflowWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInRealCodeNoMatterHowWideYourMonitorIs.class.getSimpleName().length());
        Assertions.assertTrue(IdentifierValidation.MAX_WORKFLOW_CLASS_NAME_LENGTH < TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class.getSimpleName().length());

        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateWorkflowName(TestWorkflowWithValidClassName.class));
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateWorkflowName(TestWorkflowWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInRealCodeNoMatterHowWideYourMonitorIs.class));
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> IdentifierValidation.validateWorkflowName(TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class));
    }

    public static class TestWorkflowStepWithValidClassName implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    public static class TestWorkflowStepWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInCodeNoMatterHowWideYourMonitorIs implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    public static class TestWorkflowStepWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    @Test
    public void testWorkflowStepClassName() {
        // these two checks just ensure the test will break if the max name length constant or the class names get changed.
        Assertions.assertEquals(IdentifierValidation.MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH, TestWorkflowWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInRealCodeNoMatterHowWideYourMonitorIs.class.getSimpleName().length());
        Assertions.assertTrue(IdentifierValidation.MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH < TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class.getSimpleName().length());

        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateStepName(TestWorkflowStepWithValidClassName.class));
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateStepName(TestWorkflowStepWithTechnicallyValidClassNameEvenThoughThisClassNameIsWayTooLongToBePracticalInCodeNoMatterHowWideYourMonitorIs.class));
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> IdentifierValidation.validateStepName(TestWorkflowStepWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class));
    }

    @Test
    public void testInternalValidate_RejectsEachInvalidCharacter() {
        for (int i = 0; i < invalidCharacters.length(); i++) {
            String id = invalidCharacters.substring(i, i+1);
            Assertions.assertThrows(IllegalArgumentException.class,
                                    () -> IdentifierValidation.validate("test", id, 256, true, true));
        }
    }
    @Test
    public void testInternalValidate_AllowsEachInvalidCharacterIfFlagDisabled() {
        for (int i = 0; i < invalidCharacters.length(); i++) {
            String id = invalidCharacters.substring(i, i+1);
            Assertions.assertDoesNotThrow(() -> IdentifierValidation.validate("test", id, 256, false, false));
        }
    }

    private void doCommonTests(Consumer<String> validationMethod, int maxLength, boolean allowInvalidCharacters) {
        // first verify that it accepts valid identifiers
        String validId = generateString(maxLength, allowInvalidCharacters);
        Assertions.assertDoesNotThrow(() -> validationMethod.accept(validId));

        // verify that null/empty are disallowed
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> validationMethod.accept(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> validationMethod.accept(""));

        // if invalid characters are disallowed, verify they are rejected
        if (!allowInvalidCharacters) {
            String id = generateString(maxLength, true);
            Assertions.assertThrows(IllegalArgumentException.class,
                                    () -> validationMethod.accept(id));
        }

        // verify that too-long identifiers are rejected
        String tooLongId = generateString(maxLength+1, allowInvalidCharacters);
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> validationMethod.accept(tooLongId));
    }

    private String generateString(int length, boolean includeInvalidCharacters) {
        Assertions.assertTrue(length > 0);

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            // if we're supposed to include invalid characters, we'll grab one from this list:
            // \u0000-\u001f\u007f-\u009f:/|
            // ... for every other character, starting with the first.
            if (includeInvalidCharacters && i % 2 == 0) {
                int p = ThreadLocalRandom.current().nextInt(invalidCharacters.length());
                result.append(invalidCharacters.charAt(p));
            } else {
                result.append("a");
            }
        }
        return result.toString();
    }
}
