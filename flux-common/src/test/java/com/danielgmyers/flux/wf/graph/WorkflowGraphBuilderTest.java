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

package com.danielgmyers.flux.wf.graph;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.danielgmyers.flux.step.Attribute;
import com.danielgmyers.flux.step.CloseWorkflow;
import com.danielgmyers.flux.step.PartitionIdGenerator;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.graph.teststeps.TestBranchStep;
import com.danielgmyers.flux.wf.graph.teststeps.TestOtherBranchStep;
import com.danielgmyers.flux.wf.graph.teststeps.TestPartitionedStep;
import com.danielgmyers.flux.wf.graph.teststeps.TestPartitionedStepUsesPartitionIdGeneratorResult;
import com.danielgmyers.flux.wf.graph.teststeps.TestPartitionedStepWithExtraInput;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepAlsoDeclaresOutputAttribute;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepDeclaresOutputAttribute;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepExpectsLongInputAttribute;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepExpectsStartTimeAttributesAsDate;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepExpectsStartTimeAttributesAsInstant;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepHasInputAttribute;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepHasOptionalInputAttribute;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepOne;
import com.danielgmyers.flux.wf.graph.teststeps.TestStepTwo;
import com.danielgmyers.metrics.MetricRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkflowGraphBuilderTest {

    @Test
    public void builder_FirstStepCannotBeNull() {
        try {
            new WorkflowGraphBuilder(null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void builder_FirstStepMustHaveAStepApplyMethod() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
        };

        try {
            new WorkflowGraphBuilder(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void builder_FirstStepMustHaveOnlyOneStepApplyMethod() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing1() {
            }
            @StepApply
            public void doThing2() {
            }
        };

        try {
            new WorkflowGraphBuilder(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void builder_FirstStepCannotExtendCloseWorkflowInterface() {
        WorkflowStep doNotDoThis = new CloseWorkflow() {
            @StepApply
            public void doThing() {
            }
        };

        try {
            new WorkflowGraphBuilder(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsForNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepImplementsCloseWorkflowInterface() {
        WorkflowStep doNotDoThis = new CloseWorkflow() {
            @StepApply
            public void doThing() {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfFirstStepAddedAgain() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(one);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepAddedTwice() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStep(two);

        try {
            builder.addStep(two);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepDoesNotHaveStepApplyMethod() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepHasMoreThanOneStepApplyMethod() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing1() {
            }
            @StepApply
            public void doThing2() {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepMethodHasParameterWithoutAttributeAnnotation() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing(String someParameterWithoutAttributeAnnotation) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfStepMethodHasOneParameterWithAndOneParameterWithoutAttributeAnnotation() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing(String parameterWithoutAnnotation, @Attribute("inputField") String parameterWithAnnotation) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfNonPartitionedStepRequestsPartitionIdParameter() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfNonPartitionedStepRequestsPartitionCountParameter() {
        WorkflowStep doNotDoThis = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfPartitionedStepMissingPartitionIdParameter() {
        WorkflowStep doNotDoThis = new PartitionedWorkflowStep() {
            @PartitionIdGenerator
            public List<String> partitionIds() {
                return Collections.singletonList("1");
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_COUNT) Long partitionCount) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfPartitionedStepHasPartitionCountParameterWithWrongType() {
        WorkflowStep doNotDoThis = new PartitionedWorkflowStep() {
            @PartitionIdGenerator
            public List<String> partitionIds() {
                return Collections.singletonList("1");
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                                @Attribute(StepAttributes.PARTITION_COUNT) Boolean partitionCount) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfPartitionedStepHasPartitionIdGeneratorWithPartitionIdAttribute() {
        WorkflowStep doNotDoThis = new PartitionedWorkflowStep() {
            @PartitionIdGenerator
            public List<String> partitionIds(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
                return Collections.singletonList("1");
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_ThrowsIfPartitionedStepHasPartitionIdGeneratorWithPartitionCountAttribute() {
        WorkflowStep doNotDoThis = new PartitionedWorkflowStep() {
            @PartitionIdGenerator
            public List<String> partitionIds(@Attribute(StepAttributes.PARTITION_COUNT) String partitionId) {
                return Collections.singletonList("1");
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.addStep(doNotDoThis);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void addStep_SucceedsIfStepMethodHasTwoParametersWithAttributeAnnotation() {
        WorkflowStep doThing = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute("inputField") String parameterWithAnnotation,
                                @Attribute("anotherInputField") String anotherParameterWithAnnotation) {
            }
        };

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStep(doThing);
    }

    @Test
    public void addStep_SucceedsForBasicPartitionedStep() {
        WorkflowStep doThing = new TestPartitionedStep();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStep(doThing);
    }

    @Test
    public void addStep_SucceedsForBasicPartitionedStepWithCustomPartitionIdParameters() {
        WorkflowStep doThing = new TestPartitionedStepWithExtraInput();

        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.addStep(doThing);
    }

    @Test
    public void alwaysTransition() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestStepOne.class, CloseWorkflow.class);

        verifyAlwaysTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void alwaysCloseTransition_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        verifyAlwaysTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void alwaysCloseTransition_DisallowAlwaysCloseAndAnotherCloseTransitionTogether() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        try {
            builder.closeOnSuccess(one);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_AllowForPartitionedStep() {
        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestPartitionedStep.class, CloseWorkflow.class);

        verifyAlwaysTransition(builder.build(), TestPartitionedStep.class, null);
    }

    @Test
    public void alwaysCloseTransition_DisallowAlwaysCloseAndAnotherTransitionTogether() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        try {
            builder.successTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void successTransition() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, CloseWorkflow.class);

        verifySuccessTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void successCloseTransition_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.closeOnSuccess(one);

        verifySuccessTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void successTransition_AllowForPartitionedStep() {
        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestPartitionedStep.class, CloseWorkflow.class);

        verifySuccessTransition(builder.build(), TestPartitionedStep.class, null);
    }

    @Test
    public void failTransition() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.failTransition(TestStepOne.class, CloseWorkflow.class);

        verifyFailTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void failCloseTransition_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.closeOnFailure(one);

        verifyFailTransition(builder.build(), TestStepOne.class, null);
    }

    @Test
    public void failTransition_AllowForPartitionedStep() {
        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.failTransition(TestPartitionedStep.class, CloseWorkflow.class);

        verifyFailTransition(builder.build(), TestPartitionedStep.class, null);
    }

    @Test
    public void customTransition() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        String resultCode = "customCode";
        builder.customTransition(TestStepOne.class, resultCode, CloseWorkflow.class);

        verifyCustomTransition(builder.build(), TestStepOne.class, resultCode, null);
    }

    @Test
    public void customTransition_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        String resultCode = "customCode";
        builder.customTransition(one, resultCode, two);

        builder.addStep(two);
        builder.closeOnSuccess(two);

        verifyCustomTransition(builder.build(), TestStepOne.class, resultCode, two);
    }

    @Test
    public void customCloseTransition_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        String resultCode = "customCode";
        builder.closeOnCustom(one, resultCode);

        verifyCustomTransition(builder.build(), TestStepOne.class, resultCode, null);
    }

    @Test
    public void customTransition_DisallowForPartitionedStep() {
        WorkflowStep one = new TestPartitionedStep();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestPartitionedStep.class, "customCode", CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_ThrowIfResultCodeIsNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestStepOne.class, null, CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_ThrowIfResultCodeIsBlank() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestStepOne.class, "", CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndSuccessTogether_SuccessFirst() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.alwaysTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndSuccessTogether_SuccessSecond() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.successTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndFailureTogether_FailureFirst() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.failTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.alwaysTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndFailureTogether_FailureSecond() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.failTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndCustomTogether_CustomFirst() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.customTransition(TestStepOne.class, "someCode", TestStepTwo.class);

        try {
            builder.alwaysTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowAlwaysAndCustomTogether_CustomSecond() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.customTransition(TestStepOne.class, "someCode", TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_DisallowMultipleTransitionsWithSameCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.alwaysTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void successTransition_DisallowMultipleTransitionsWithSameCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.successTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void failTransition_DisallowMultipleTransitionsWithSameCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.failTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.failTransition(TestStepOne.class, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_DisallowMultipleTransitionsWithSameCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        String customCode = "someMagicCode";
        builder.customTransition(TestStepOne.class, customCode, TestStepTwo.class);

        try {
            builder.customTransition(TestStepOne.class, customCode, TestBranchStep.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_ThrowIfSourceStepNotAddedYet() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.alwaysTransition(TestStepTwo.class, CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void successTransition_ThrowIfSourceStepNotAddedYet() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.successTransition(TestStepTwo.class, CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void failTransition_ThrowIfSourceStepNotAddedYet() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.failTransition(TestStepTwo.class, CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_ThrowIfSourceStepNotAddedYet() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestStepTwo.class, "someCode", CloseWorkflow.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_ThrowIfTargetIsNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.alwaysTransition(TestStepOne.class, null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void successTransition_ThrowIfTargetIsNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.successTransition(TestStepOne.class, null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void failTransition_ThrowIfTargetIsNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.failTransition(TestStepOne.class, null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_ThrowIfTargetIsNull() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestStepOne.class, "customCode", null);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void alwaysTransition_ThrowIfTargetIsSameAsSource() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.alwaysTransition(TestStepOne.class, TestStepOne.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void successTransition_ThrowIfTargetIsSameAsSource() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.successTransition(TestStepOne.class, TestStepOne.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void failTransition_ThrowIfTargetIsSameAsSource() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.failTransition(TestStepOne.class, TestStepOne.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void customTransition_ThrowIfTargetIsSameAsSource() {
        WorkflowStep one = new TestStepOne();
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.customTransition(TestStepOne.class, "customCode", TestStepOne.class);
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void buildTrivialGraph() {
        WorkflowStep one = new TestStepOne();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, CloseWorkflow.class);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(1, graph.getNodes().size());
        verifySuccessTransition(graph, TestStepOne.class, null);
    }

    @Test
    public void buildLinearGraph() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);

        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, CloseWorkflow.class);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(2, graph.getNodes().size());
        verifySuccessTransition(graph, TestStepOne.class, two);
        verifySuccessTransition(graph, TestStepTwo.class, null);
    }

    @Test
    public void buildLinearGraph_byReference() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(one, two);

        builder.addStep(two);
        builder.closeOnSuccess(two);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(2, graph.getNodes().size());
        verifySuccessTransition(graph, TestStepOne.class, two);
        verifySuccessTransition(graph, TestStepTwo.class, null);
    }

    @Test
    public void buildBranchingGraph_FailureResultCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.failTransition(TestStepOne.class, TestBranchStep.class);

        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, CloseWorkflow.class);

        builder.addStep(branch);
        builder.successTransition(TestBranchStep.class, CloseWorkflow.class);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(3, graph.getNodes().size());

        Map<String, WorkflowStep> stepOneTransitions = new HashMap<>();
        stepOneTransitions.put(StepResult.SUCCEED_RESULT_CODE, two);
        stepOneTransitions.put(StepResult.FAIL_RESULT_CODE, branch);
        verifyTransitions(graph, TestStepOne.class, stepOneTransitions);

        verifySuccessTransition(graph, TestStepTwo.class, null);
        verifySuccessTransition(graph, TestBranchStep.class, null);
    }

    @Test
    public void buildBranchingGraph_FailureResultCode_UsesCommonTransitionMethod() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.commonTransitions(one, two, branch);

        builder.addStep(two);
        builder.closeOnSuccess(two);

        builder.addStep(branch);
        builder.closeOnSuccess(branch);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(3, graph.getNodes().size());

        Map<String, WorkflowStep> stepOneTransitions = new HashMap<>();
        stepOneTransitions.put(StepResult.SUCCEED_RESULT_CODE, two);
        stepOneTransitions.put(StepResult.FAIL_RESULT_CODE, branch);
        verifyTransitions(graph, TestStepOne.class, stepOneTransitions);

        verifySuccessTransition(graph, TestStepTwo.class, null);
        verifySuccessTransition(graph, TestBranchStep.class, null);
    }

    @Test
    public void buildBranchingGraph_CustomResultCode() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();

        String branchCode = "branchme";

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.customTransition(TestStepOne.class, branchCode, TestBranchStep.class);

        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, CloseWorkflow.class);

        builder.addStep(branch);
        builder.successTransition(TestBranchStep.class, CloseWorkflow.class);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(3, graph.getNodes().size());

        Map<String, WorkflowStep> stepOneTransitions = new HashMap<>();
        stepOneTransitions.put(StepResult.SUCCEED_RESULT_CODE, two);
        stepOneTransitions.put(branchCode, branch);
        verifyTransitions(graph, TestStepOne.class, stepOneTransitions);

        verifySuccessTransition(graph, TestStepTwo.class, null);
        verifySuccessTransition(graph, TestBranchStep.class, null);
    }

    @Test
    public void buildBranchingGraph_NoLoop_BranchesConverge() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();
        WorkflowStep otherBranch = new TestOtherBranchStep();

        String branchCode = "branchme";

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestBranchStep.class);
        builder.customTransition(TestStepOne.class, branchCode, TestOtherBranchStep.class);

        builder.addStep(branch);
        builder.successTransition(TestBranchStep.class, TestStepTwo.class);

        builder.addStep(otherBranch);
        builder.successTransition(TestOtherBranchStep.class, TestStepTwo.class);

        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, CloseWorkflow.class);

        WorkflowGraph graph = builder.build();

        Assertions.assertEquals(one, graph.getFirstStep());

        Assertions.assertEquals(4, graph.getNodes().size());

        Map<String, WorkflowStep> stepOneTransitions = new HashMap<>();
        stepOneTransitions.put(StepResult.SUCCEED_RESULT_CODE, branch);
        stepOneTransitions.put(branchCode, otherBranch);
        verifyTransitions(graph, TestStepOne.class, stepOneTransitions);

        verifySuccessTransition(graph, TestBranchStep.class, two);
        verifySuccessTransition(graph, TestOtherBranchStep.class, two);

        verifySuccessTransition(graph, TestStepTwo.class, null);
    }

    @Test
    public void build_FailsIfNoTransitionsDefinedForOnlyStep() {
        WorkflowStep one = new TestStepOne();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfNoTransitionsDefinedForLaterStep() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.addStep(two);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfTransitionTargetIsNotDefined() {
        WorkflowStep one = new TestStepOne();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfANodeIsUnreachable() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, CloseWorkflow.class);

        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, CloseWorkflow.class);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfGraphHasLoop_TrivialCase() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, TestStepOne.class);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfGraphHasLoop_LoopNotToFirstStep() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();
        WorkflowStep otherBranch = new TestOtherBranchStep();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestStepTwo.class);
        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, TestBranchStep.class);
        builder.addStep(branch);
        builder.successTransition(TestBranchStep.class, TestOtherBranchStep.class);
        builder.addStep(otherBranch);
        builder.successTransition(TestOtherBranchStep.class, TestStepTwo.class);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_FailsIfGraphHasLoop_LoopInBranch() {
        WorkflowStep one = new TestStepOne();
        WorkflowStep two = new TestStepTwo();
        WorkflowStep branch = new TestBranchStep();
        WorkflowStep otherBranch = new TestOtherBranchStep();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.successTransition(TestStepOne.class, TestOtherBranchStep.class);
        builder.failTransition(TestStepOne.class, TestStepTwo.class);
        builder.addStep(two);
        builder.successTransition(TestStepTwo.class, TestBranchStep.class);

        builder.addStep(branch);
        builder.successTransition(TestBranchStep.class, TestStepOne.class);

        builder.addStep(otherBranch);
        builder.successTransition(TestOtherBranchStep.class, CloseWorkflow.class);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesNotValidateAttributeInputsIfInitialInputAttributesNotSpecified() {
        WorkflowStep one = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one);
        builder.alwaysClose(one);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailable() {
        WorkflowStep one = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailableButOptional() {
        WorkflowStep one = new TestStepHasOptionalInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        // this one should succeed even though the attribute is not available, because the input attribute is optional
        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableInInitialInput() {
        WorkflowStep one = new TestStepHasInputAttribute();

        Map<String, Class<?>> initialAttributes = new HashMap<>();
        initialAttributes.put(TestStepHasInputAttribute.INPUT_ATTR, String.class);
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, initialAttributes);
        builder.alwaysClose(one);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromPreviousStep() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromPreviousPartitionIdGenerator() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("1");
        partitionIds.add("2");
        WorkflowStep two = new TestPartitionedStepUsesPartitionIdGeneratorResult(partitionIds);
        WorkflowStep three = new WorkflowStep() {
            @StepApply
            public void apply(@Attribute(TestPartitionedStepUsesPartitionIdGeneratorResult.PARTITION_ID_GENERATOR_RESULT_ATTRIBUTE) String attr) {

            }
        };

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysTransition(two, three);

        builder.addStep(three);
        builder.alwaysClose(three);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableButWrongType() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepExpectsLongInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowOverwritingAttribute() {
        WorkflowStep one = new TestStepDeclaresOutputAttribute();
        WorkflowStep two = new TestStepAlsoDeclaresOutputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysTransition(one, two);

        builder.addStep(two);
        builder.alwaysClose(two);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeNotAvailableFromOneBranch() {
        WorkflowStep start = new TestStepOne();
        WorkflowStep branchA = new TestStepDeclaresOutputAttribute();
        WorkflowStep branchB = new TestStepTwo();
        WorkflowStep end = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.customTransition(start, "foo", branchA);
        builder.customTransition(start, "bar", branchB);

        builder.addStep(branchA);
        builder.alwaysTransition(branchA, end);

        builder.addStep(branchB);
        builder.alwaysTransition(branchB, end);

        builder.addStep(end);
        builder.alwaysClose(end);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_OptionalAttributeNotAvailableFromOneBranch() {
        WorkflowStep start = new TestStepOne();
        WorkflowStep branchA = new TestStepDeclaresOutputAttribute();
        WorkflowStep branchB = new TestStepTwo();
        WorkflowStep end = new TestStepHasOptionalInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.customTransition(start, "foo", branchA);
        builder.customTransition(start, "bar", branchB);

        builder.addStep(branchA);
        builder.alwaysTransition(branchA, end);

        builder.addStep(branchB);
        builder.alwaysTransition(branchB, end);

        builder.addStep(end);
        builder.alwaysClose(end);

        // this one should succeed even though the attribute is not available, because the input attribute is optional
        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromBothBranches() {
        WorkflowStep start = new TestStepOne();
        WorkflowStep branchA = new TestStepDeclaresOutputAttribute();
        WorkflowStep branchB = new TestStepAlsoDeclaresOutputAttribute();
        WorkflowStep end = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.customTransition(start, "foo", branchA);
        builder.customTransition(start, "bar", branchB);

        builder.addStep(branchA);
        builder.alwaysTransition(branchA, end);

        builder.addStep(branchB);
        builder.alwaysTransition(branchB, end);

        builder.addStep(end);
        builder.alwaysClose(end);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AttributeAvailableFromBeforeBranch() {
        WorkflowStep start = new TestStepDeclaresOutputAttribute();
        WorkflowStep branchA = new TestStepOne();
        WorkflowStep branchB = new TestStepTwo();
        WorkflowStep end = new TestStepHasInputAttribute();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.customTransition(start, "foo", branchA);
        builder.customTransition(start, "bar", branchB);

        builder.addStep(branchA);
        builder.alwaysTransition(branchA, end);

        builder.addStep(branchB);
        builder.alwaysTransition(branchB, end);

        builder.addStep(end);
        builder.alwaysClose(end);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_DisallowsStartTimeAttributesAsDate() {
        WorkflowStep start = new TestStepExpectsStartTimeAttributesAsDate();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.alwaysClose(start);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_DoesValidateAttributeInputsIfInitialInputAttributesSpecified_AllowsStartTimeAttributesAsInstant() {
        WorkflowStep start = new TestStepExpectsStartTimeAttributesAsInstant();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(start, Collections.emptyMap());
        builder.alwaysClose(start);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_ValidatesRequiredPartitionedStepInputs() {
        WorkflowStep one = new TestPartitionedStep();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_ValidatesRequiredPartitionedStepInputs_RequiredAttributeAvailable() {
        WorkflowStep one = new TestPartitionedStepWithExtraInput();

        Map<String, Class<?>> initialAttributes = new HashMap<>();
        initialAttributes.put(TestPartitionedStepWithExtraInput.INPUT_ATTR, String.class);
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, initialAttributes);
        builder.alwaysClose(one);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_ValidatesRequiredPartitionedStepInputs_RequiredAttributeNotAvailable() {
        WorkflowStep one = new TestPartitionedStepWithExtraInput();

        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(one, Collections.emptyMap());
        builder.alwaysClose(one);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_ValidateAttributeInputs_AllowWorkflowId() {
        WorkflowStep alwaysAllowWorkflowId = new PartitionedWorkflowStep() {

            @PartitionIdGenerator
            public PartitionIdGeneratorResult getPartitionIds(@Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
                return PartitionIdGeneratorResult.create();
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId,
                                @Attribute(StepAttributes.WORKFLOW_ID) String workflowId) {
            }
        };

        // note we aren't passing the workflow id attribute to the builder
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(alwaysAllowWorkflowId, Collections.emptyMap());
        builder.alwaysClose(alwaysAllowWorkflowId);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_DisallowActivityInitialTimeAsDate() {
        WorkflowStep alwaysAllowInitialAttemptTime = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Date initialAttemptTime) {
            }
        };

        // note we aren't passing the activity initial attempt time attribute to the builder
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(alwaysAllowInitialAttemptTime, Collections.emptyMap());
        builder.alwaysClose(alwaysAllowInitialAttemptTime);

        try {
            builder.build();
            Assertions.fail();
        } catch (WorkflowGraphBuildException e) {
            // expected
        }
    }

    @Test
    public void build_ValidateAttributeInputs_AllowActivityInitialTimeAsInstant() {
        WorkflowStep alwaysAllowInitialAttemptTime = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute(StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME) Instant initialAttemptTime) {
            }
        };

        // note we aren't passing the activity initial attempt time attribute to the builder
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(alwaysAllowInitialAttemptTime, Collections.emptyMap());
        builder.alwaysClose(alwaysAllowInitialAttemptTime);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_AllowRetryAttempt() {
        WorkflowStep alwaysAllowRetryAttempt = new WorkflowStep() {
            @StepApply
            public void doThing(@Attribute(StepAttributes.RETRY_ATTEMPT) Long retryAttempt) {
            }
        };

        // note we aren't passing the activity initial attempt time attribute to the builder
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(alwaysAllowRetryAttempt, Collections.emptyMap());
        builder.alwaysClose(alwaysAllowRetryAttempt);

        Assertions.assertNotNull(builder.build());
    }

    @Test
    public void build_ValidateAttributeInputs_AllowMetrics() {
        WorkflowStep alwaysAllowMetrics = new PartitionedWorkflowStep() {

            @PartitionIdGenerator
            public PartitionIdGeneratorResult getPartitionIds(MetricRecorder metrics) {
                return PartitionIdGeneratorResult.create();
            }

            @StepApply
            public void doThing(@Attribute(StepAttributes.PARTITION_ID) String partitionId, MetricRecorder metrics) {
            }
        };

        // note we aren't passing the activity initial attempt time attribute to the builder
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(alwaysAllowMetrics, Collections.emptyMap());
        builder.alwaysClose(alwaysAllowMetrics);

        Assertions.assertNotNull(builder.build());
    }

    private void verifyAlwaysTransition(WorkflowGraph graph, Class<? extends WorkflowStep> source, WorkflowStep successTarget) {
        verifyCustomTransition(graph, source, StepResult.ALWAYS_RESULT_CODE, successTarget);
    }

    private void verifySuccessTransition(WorkflowGraph graph, Class<? extends WorkflowStep> source, WorkflowStep successTarget) {
        verifyCustomTransition(graph, source, StepResult.SUCCEED_RESULT_CODE, successTarget);
    }

    private void verifyFailTransition(WorkflowGraph graph, Class<? extends WorkflowStep> source, WorkflowStep successTarget) {
        verifyCustomTransition(graph, source, StepResult.FAIL_RESULT_CODE, successTarget);
    }

    private void verifyCustomTransition(WorkflowGraph graph, Class<? extends WorkflowStep> source,
                                        String resultCode, WorkflowStep successTarget) {
        Assertions.assertTrue(graph.getNodes().containsKey(source));
        WorkflowGraphNode node = graph.getNodes().get(source);
        Assertions.assertEquals(1, node.getNextStepsByResultCode().size());
        Assertions.assertTrue(node.getNextStepsByResultCode().containsKey(resultCode));
        if(successTarget == null) {
            Assertions.assertNull(node.getNextStepsByResultCode().get(resultCode));
        } else {
            Assertions.assertEquals(successTarget, node.getNextStepsByResultCode().get(resultCode).getStep());
        }
    }

    private void verifyTransitions(WorkflowGraph graph, Class<? extends WorkflowStep> source,
                                   Map<String, WorkflowStep> targetsByResultCode) {
        Assertions.assertTrue(graph.getNodes().containsKey(source));
        WorkflowGraphNode node = graph.getNodes().get(source);
        Assertions.assertEquals(targetsByResultCode.size(), node.getNextStepsByResultCode().size());
        Assertions.assertTrue(node.getNextStepsByResultCode().keySet().containsAll(targetsByResultCode.keySet()));

        for (Entry<String, WorkflowStep> entry : targetsByResultCode.entrySet()) {
            if(entry.getValue() == null) {
                Assertions.assertNull(node.getNextStepsByResultCode().get(entry.getKey()));
            } else {
                Assertions.assertEquals(entry.getValue(), node.getNextStepsByResultCode().get(entry.getKey()).getStep());

            }
        }
    }
}
