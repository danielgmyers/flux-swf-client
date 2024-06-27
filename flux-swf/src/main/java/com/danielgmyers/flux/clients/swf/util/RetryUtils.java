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

package com.danielgmyers.flux.clients.swf.util;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.danielgmyers.flux.clients.swf.FluxCapacitorImpl;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.WorkflowStepUtil;
import com.danielgmyers.metrics.MetricRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Utilities for retrying operations and calculating backoff times.
 */
public final class RetryUtils {

    private static final Logger log = LoggerFactory.getLogger(RetryUtils.class);

    private RetryUtils() {}

    private static final String THROTTLING_MESSAGE = "rate exceeded";
    private static final Set<Class<?>> RETRYABLE_INNER_CLIENT_EXCEPTIONS = new HashSet<>();

    static {
        RETRYABLE_INNER_CLIENT_EXCEPTIONS.add(java.net.SocketException.class);
    }

    /**
     * Retries a given function up to maxAttempts times, with exponential backoff and jitter, a minimum retry delay
     * of 100ms, and a maximum retry delay of maxRetryDelay.
     * It records time and various count metrics using the specified operation prefix.
     */
    public static <T> T executeWithInlineBackoff(Supplier<T> function, long maxAttempts, Duration maxRetryDelay,
                                                 MetricRecorder metrics, String operationPrefix) {
        return executeWithInlineBackoff(function, maxAttempts, Duration.ofMillis(100), maxRetryDelay, metrics, operationPrefix);
    }

    /**
     * Retries a given function up to maxAttempts times, with exponential backoff and jitter, a minimum
     * retry delay of minRetryDelay, and a maximum retry delay of maxRetryDelay.
     * It records time and various count metrics using the specified operation prefix.
     */
    public static <T> T executeWithInlineBackoff(Supplier<T> function, long maxAttempts, Duration minRetryDelay,
                                                 Duration maxRetryDelay, MetricRecorder metrics, String operationPrefix) {
        String timeMetricName = operationPrefix + "Time";
        String callCountMetricName = operationPrefix + "CallCount";
        String failureCountMetricName = operationPrefix + "Error";
        String throttleCountMetricName = operationPrefix + "Throttled";
        String retryableClientExceptionCountMetricName = operationPrefix + "RetryableClientException";
        String gaveUpRetryingThrottlingMetricName = operationPrefix + "ThrottledTooManyTimes";
        String gaveUpRetryingClientExceptionMetricName = operationPrefix + "RetriedClientExceptionTooManyTimes";
        int retry = 0;
        while (true) {
            try {
                metrics.addCount(callCountMetricName, 1);
                metrics.startDuration(timeMetricName);
                final T result = function.get();
                metrics.endDuration(timeMetricName);
                metrics.addCount(throttleCountMetricName, 0);
                metrics.addCount(retryableClientExceptionCountMetricName, 0);
                metrics.addCount(gaveUpRetryingThrottlingMetricName, 0);
                metrics.addCount(gaveUpRetryingClientExceptionMetricName, 0);
                metrics.addCount(failureCountMetricName, 0);
                log.debug("Succeeded at executing the request after {} retries.", retry);
                return result;
            } catch (SdkServiceException e) {
                // make sure the time duration gets closed out before we sleep/retry
                metrics.endDuration(timeMetricName);

                if (!e.isThrottlingException()) {
                    // SWF doesn't (always?) use status code 429 for throttling, so we have to check the message too :(
                    String message = e.getMessage();
                    if (message == null || !message.toLowerCase().contains(THROTTLING_MESSAGE)) {
                        metrics.addCount(failureCountMetricName, 1);
                        throw e;
                    }
                }
                metrics.addCount(throttleCountMetricName, 1);
                retry++;
                if (retry >= maxAttempts) {
                    log.debug("Still throttled after {} attempts. Giving up on retrying with this thread.", maxAttempts);
                    metrics.addCount(gaveUpRetryingThrottlingMetricName, 1);
                    throw e;
                }

                // we'll always start backing off after the second retry, up to maxRetryDelay per retry, with 10% jitter.
                // we'll treat this as milliseconds and sleep for this long.
                long sleepTimeMillis = calculateRetryBackoff(1, minRetryDelay.toMillis(), maxRetryDelay.toMillis(), retry, 10,
                    FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
                log.debug("Throttled by SWF, retrying after {}ms.", sleepTimeMillis);
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            } catch (SdkClientException e) {
                Throwable retryableInnerException = getInnerRetryableClientException(e);
                if (retryableInnerException == null) {
                    throw e;
                }
                metrics.endDuration(timeMetricName);


                metrics.addCount(retryableClientExceptionCountMetricName, 1);
                retry++;
                if (retry >= maxAttempts) {
                    log.debug("Still has retryable client exception after {} attempts."
                              + " Giving up on retrying with this thread.", maxAttempts);
                    metrics.addCount(gaveUpRetryingClientExceptionMetricName, 1);
                    throw e;
                }

                // we'll always start backing off after the second retry, up to maxRetryDelay per retry, with 10% jitter.
                // we'll treat this as milliseconds and sleep for this long.
                long sleepTimeMillis = calculateRetryBackoff(1, minRetryDelay.toMillis(), maxRetryDelay.toMillis(), retry, 10,
                    FluxCapacitorImpl.DEFAULT_EXPONENTIAL_BACKOFF_BASE);
                log.debug("Retryable client Exception, retrying after {}ms.", sleepTimeMillis);
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }
    }

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

    private static long calculateRetryBackoff(long retriesBeforeBackoff, long initialRetryDelay, long maxRetryDelay,
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

    private static Throwable getInnerRetryableClientException(SdkClientException ex) {
        if (ex == null) {
            return null;
        }

        Throwable cause = ex.getCause();
        while (cause != null) {
            Class<?> clazz = cause.getClass();
            if (RETRYABLE_INNER_CLIENT_EXCEPTIONS.stream()
                .anyMatch(retryableException -> retryableException.isAssignableFrom(clazz))
            ) {
                return cause;
            }
            cause = cause.getCause();
        }

        return null;
    }

}
