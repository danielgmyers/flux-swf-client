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

package software.amazon.aws.clients.swf.flux.step;

/**
 * An interface marking a class to be a *partitioned* workflow step.
 * As with WorkflowStep, the class must define exactly one public method with the @StepApply annotation.
 *
 * A partitioned workflow step is a step where multiple copies of the step are executed in parallel with slightly different
 * input. For example, an order validation workflow might include a step that validates item availability for each
 * item in parallel, where each partition handles a single item.
 *
 * Any step that implements this interface must implement a method with the @PartitionIdGenerator annotation.
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
 * - FAILURE: the entire step is failed if a single partition fails. Other partitions will continue to retry until
 *            they either succeed or fail.
 * - SUCCESS: the step does not succeed until all of its partitions have succeeded.
 * - RETRY: each partition will retry as needed until it succeeds or fails.
 *
 * Custom result codes are specifically disallowed for partitioned workflow steps.
 *
 * Do not attempt to convert an existing step from PartitionedWorkflowStep to WorkflowStep, or vice versa; this would
 * result in an invalid workflow state.
 *
 * If it is important to use the same set of partition IDs for multiple steps in a workflow, there are two solutions,
 * depending on the nature of the partition IDs:
 * - If the partition IDs are static (or won't change during the workflow), the same partition ID generation code can be used,
 *   either via a utility method or by sharing a base class between the steps in question.
 * - If the calculated set of partition IDs can change from one execution to another, then the workflow should determine
 *   the set of partition IDs once and store them elsewhere for lookup in the @PartitionIdGenerator methods.
 */
public interface PartitionedWorkflowStep extends WorkflowStep {
}
