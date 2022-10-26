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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Required on each parameter of a method that has any of these annotations:
 * - @StepApply
 * - @PartitionIdGenerator
 * - @StepHook
 * Tells FluxCapacitor which field from the input map to pass in to a particular field, if present, otherwise null will be passed.
 * FluxCapacitor will attempt to deserialize the specified input field as the type of the associated parameter.
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Attribute {

    /**
     * The field to pull from the input map for the associated parameter.
     */
    String value();

    /**
     * Whether the input is optional; only used when validating the workflow graph at graph build time, not during normal execution.
     */
    boolean optional() default false;
}
