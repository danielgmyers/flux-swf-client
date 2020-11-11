/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.aws.clients.swf.flux.step;

/**
 * An interface marking a class to be a *partitioned* workflow step.
 * As with WorkflowStep, the class must define exactly one public method with the @StepApply annotation.
 *
 * In addition, the step must implement a method with the @PartitionIdGenerator annotation.
 *
 * PartitionedWorkflowSteps may override declaredOutputAttributes to declare that their @PartitionIdGenerator method
 * returns the declared attributes. If their @StepApply method includes any output attributes they are ignored.
 *
 * The @StepApply method's parameter list *must* include a parameter annotated with @Attribute(StepAttributes.PARTITION_ID).
 * If the parameter with that annotation is not a String, the library will attempt to deserialize the partition id
 * as the annotated parameter's type.
 *
 * The @StepApply method may also include a Long parameter annotated with @Attribute(StepAttributes.PARTITION_COUNT)
 * to be provided the number of partitions that were created.
 *
 * Partition result codes are handled as follows:
 * - FAILURE: the entire step is failed if a single partition fails. Partitions that are already scheduled may or may not execute.
 * - SUCCESS: the step does not succeed until all of its partitions have succeeded.
 * - RETRY: each partition will retry as needed until it succeeds or fails.
 */
public interface PartitionedWorkflowStep extends WorkflowStep {
}
