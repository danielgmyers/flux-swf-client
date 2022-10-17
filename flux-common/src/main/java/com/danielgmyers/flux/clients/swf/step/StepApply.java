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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tells FluxCapacitor which method should be called to execute the step.
 * Exactly one method should have this annotation for any class implementing the WorkflowStep interface.
 * Every parameter of the method should also have the @Attribute annotation, unless the parameter is
 * of type com.danielgmyers.flux.clients.swf.metrics.MetricRecorder.
 *
 * If a step requests a MetricRecorder object, Flux will provide one that has already been populated with a start time
 * and an Operation (containing the workflow and step name), and when the step ends, Flux will record an end time
 * and close the metrics context.
 *
 * If the annotated method throws an exception or returns StepResult.retry(), Flux will schedule another attempt
 * of the workflow step.
 *
 * By default, the initial retry delay is ten seconds, and Flux retries steps six times before starting exponential backoff.
 * Jitter is applied to the retry times (ten percent by default) to avoid large numbers of steps retrying concurrently.
 * Flux eventually stops increasing retry time once it exceeds a threshold (ten minutes by default).
 * All of these parameters can be configured on a per-workflow-step basis using the values on the @StepApply annotation.
 *
 * If the method has a return type of StepResult, the StepResult returned by the method will be respected.
 *
 * If the method successfully returns an object of some other type (or void), Flux will use StepResult.success().
 *
 * If the method throws an exception e, Flux will use StepResult.retry(e.getMessage()).
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepApply {
    /**
     * Controls the amount of time to delay before executing the first retry attempt, in seconds.
     * Defaults to 10 seconds.
     */
    long initialRetryDelaySeconds() default 10;

    /**
     * Controls the maximum allowed delay between retry attempts (before jitter is applied), in seconds.
     * Defaults to 600 seconds (10 minutes).
     */
    long maxRetryDelaySeconds() default 10 * 60;

    /**
     * Controls the amount of jitter used to randomize the retry delay (*after* clamping to the min and max values),
     * as a percentage of the calculated retry delay.
     *
     * For example, if the calculated retry delay is 30 seconds, and the jitter percent is 10,
     * then the calculated jitter amount will be somewhere between -3 seconds and 3 seconds,
     * leading to an actual retry delay of between 27 and 33 seconds (inclusive).
     *
     * Specifying 0 will disable jitter.
     */
    long jitterPercent() default 10;

    /**
     * Controls how many retries use the initial retry delay before we start doing exponential backoff.
     * Don't set it below 1, otherwise the first retry won't use the initial retry time, it will use the second retry time
     * in the exponential backoff sequence.
     *
     * To disable backoff entirely, set maxRetryDelayMillis to the same value as initialRetryDelayMillis,
     * instead of setting retriesBeforeBackoff to a high value.
     */
    long retriesBeforeBackoff() default 6;

    /**
     * Controls the multiplier applied to the exponential backoff calculation.
     *
     * If this is set to a value greater than 0.0, it overrides the global exponential backoff base (whether or not the global
     * exponential backoff base has been overridden via FluxCapacitorConfig).
     *
     * By default the backoff calculation results in an exponential curve starting from initialRetryDelaySeconds()
     * and multiplying by Math.pow(base, retryAttempt) each time, up to maxRetryDelaySeconds().
     */
    double exponentialBackoffBase() default 0.0;
}
