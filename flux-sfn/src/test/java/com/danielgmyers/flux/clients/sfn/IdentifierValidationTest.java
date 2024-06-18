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

package com.danielgmyers.flux.clients.sfn;

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
        sb.append(" \t\n");
        sb.append("<>{}[]");
        sb.append("?*");
        sb.append("\"#%\\^|~`$&,;:/");
        invalidCharacters = sb.toString();
    }

    @Test
    public void testWorkflowExecutionId() {
        doCommonTests(IdentifierValidation::validateWorkflowExecutionId, IdentifierValidation.MAX_WORKFLOW_EXECUTION_ID_LENGTH, false);
    }

    @Test
    public void testHostname() {
        doCommonTests(IdentifierValidation::validateHostname, IdentifierValidation.MAX_HOSTNAME_LENGTH, true);
    }

    @Test
    public void testPartitionId() {
        doCommonTests(IdentifierValidation::validatePartitionId, IdentifierValidation.MAX_PARTITION_ID_LENGTH, true);
    }

    public static class TestWorkflowWithValidClassName implements Workflow {
        // implementation doesn't need to be valid for this test
        @Override
        public WorkflowGraph getGraph() {
            return null;
        }
    }

    public static class TestWorkflowWithLongestAllowedClassName implements Workflow {
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
        Assertions.assertEquals(IdentifierValidation.MAX_WORKFLOW_CLASS_NAME_LENGTH, TestWorkflowWithLongestAllowedClassName.class.getSimpleName().length());
        Assertions.assertTrue(IdentifierValidation.MAX_WORKFLOW_CLASS_NAME_LENGTH < TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class.getSimpleName().length());

        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateWorkflowName(TestWorkflowWithValidClassName.class));
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateWorkflowName(TestWorkflowWithLongestAllowedClassName.class));
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> IdentifierValidation.validateWorkflowName(TestWorkflowWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class));
    }

    public static class TestWorkflowStepWithValidClassName implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    public static class WorkflowStepWithLongestAllowedClassName implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    public static class TestWorkflowStepWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely implements WorkflowStep {
        // implementation doesn't need to be valid for this test
    }

    @Test
    public void testWorkflowStepClassName() {
        // these two checks just ensure the test will break if the max name length constant or the class names get changed.
        Assertions.assertEquals(IdentifierValidation.MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH, WorkflowStepWithLongestAllowedClassName.class.getSimpleName().length());
        Assertions.assertTrue(IdentifierValidation.MAX_WORKFLOW_STEP_CLASS_NAME_LENGTH < TestWorkflowStepWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class.getSimpleName().length());

        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateStepName(TestWorkflowStepWithValidClassName.class));
        Assertions.assertDoesNotThrow(() -> IdentifierValidation.validateStepName(WorkflowStepWithLongestAllowedClassName.class));
        Assertions.assertThrows(IllegalArgumentException.class,
                                () -> IdentifierValidation.validateStepName(TestWorkflowStepWithOverlyLongClassNameThisClassNameIsMuchTooLongAndShouldNeverBeAllowedByTheValidationLogicEvenIfAskedExtremelyNicely.class));
    }

    @Test
    public void testInternalValidate_RejectsEachInvalidCharacter() {
        for (int i = 0; i < invalidCharacters.length(); i++) {
            String id = invalidCharacters.substring(i, i+1);
            Assertions.assertThrows(IllegalArgumentException.class,
                                    () -> IdentifierValidation.validate("test", id, 256, true), "failed on index " + i);
        }
    }

    @Test
    public void testInternalValidate_AllowsEachInvalidCharacterIfFlagDisabled() {
        for (int i = 0; i < invalidCharacters.length(); i++) {
            String id = invalidCharacters.substring(i, i+1);
            Assertions.assertDoesNotThrow(() -> IdentifierValidation.validate("test", id, 256, false), "failed on index " + i);
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
            // if we're supposed to include invalid characters, we'll grab a random one from the invalidCharacters variable
            // for every other character.
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
