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

import java.lang.reflect.Method;

import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.WorkflowStepUtil;

/**
 * Utilities for calculating retry backoff times.
 */
public final class RetryUtils {

    private RetryUtils() {}

    /**
     * Calculates a retry's backoff time in seconds, respecting the specified particular workflow step's
     * configured retry settings.
     */
    public static long calculateRetryBackoffInSeconds(WorkflowStep stepToRetry, long retryAttempt,
                                                      double exponentialBackoffBase) {
        Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(stepToRetry.getClass(), StepApply.class);
        StepApply applyConfig = applyMethod.getAnnotation(StepApply.class);

        double backoffBase = exponentialBackoffBase;
        if (applyConfig.exponentialBackoffBase() > 0.0) {
            backoffBase = applyConfig.exponentialBackoffBase();
        }

        return calculateRetryBackoff(applyConfig.retriesBeforeBackoff(), applyConfig.initialRetryDelaySeconds(),
                                     applyConfig.maxRetryDelaySeconds(), retryAttempt, applyConfig.jitterPercent(),
                                     backoffBase);
    }

    /**
     * Low-level helper for calculating arbitrary backoff times. Works in arbitrary units; the output will be in the same
     * units as the inputs, but unit consistency across inputs is not validated.
     */
    public static long calculateRetryBackoff(long retriesBeforeBackoff, long initialRetryDelay, long maxRetryDelay,
                                             long retryAttempt, long jitterPercent, double exponentialBackoffBase) {
        // We subtract from the retryAttempt number so that some number of retries use the initial retry time.
        // At a minimum, we want to subtract 1 so that we don't skip the first retry using the initial retry time.
        // We can adjust this to e.g. retryAttempt - 3 so that the first 3 retry attempts use the initial retry time.
        long power = Math.max(0, retryAttempt - retriesBeforeBackoff);

        long retryDelay = (long)(initialRetryDelay * Math.pow(exponentialBackoffBase, power));

        // make sure we don't go over the pre-jitter max delay
        retryDelay = Math.min(retryDelay, maxRetryDelay);

        // calculate the max jitter duration
        final double jitterMultiplier = ((double)jitterPercent / 100.0);
        final long maxJitter = (jitterPercent == 0 ? 0 : (long)(retryDelay * jitterMultiplier));

        // First we reduce retryDelay by maxJitter, then add a random integer in [0, 2 * maxJitter] to the retry time.
        retryDelay = (retryDelay - maxJitter + (long)Math.rint(2 * Math.random() * maxJitter));

        return retryDelay;
    }

}
