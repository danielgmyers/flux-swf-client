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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tells Flux which method should be called to generate the partition IDs used for a particular partitioned workflow step.
 * Exactly one method should have this annotation for any class implementing the PartitionedWorkflowStep interface.
 * Parameters to this method have the same annotation requirements as parameters to @StepApply methods.
 *
 * The return type of the method must be either List&lt;String&gt; or PartitionIdGeneratorResult.
 * The partition IDs are taken from the provided response object.
 *
 * A maximum of 1000 partition IDs may be returned. This is due to SWF's hard limit on the number of decisions that
 * can be returned as part of a decision task response.
 *
 * If the annotated method throws an exception, Flux will fail the entire decision task and try again.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionIdGenerator {
}
