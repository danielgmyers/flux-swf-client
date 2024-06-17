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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.danielgmyers.metrics.MetricRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Utilities for retrying operations and calculating backoff times.
 */
public final class AwsRetryUtils {

    private static final Logger log = LoggerFactory.getLogger(AwsRetryUtils.class);

    private AwsRetryUtils() {}

    private static final String THROTTLING_MESSAGE = "rate exceeded";
    private static final Set<Class<?>> RETRYABLE_INNER_CLIENT_EXCEPTIONS = new HashSet<>();

    private static final double INLINE_EXPONENTIAL_BACKOFF_RATE = 1.25;

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
     * Specifically handles throttling exceptions from AWS services and retryable client SDK exceptions.
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
                    // Some services don't use status code 429 for throttling, so we have to check the message too :(
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
                long sleepTimeMillis = RetryUtils.calculateRetryBackoff(1, minRetryDelay.toMillis(), maxRetryDelay.toMillis(),
                        retry, 10, INLINE_EXPONENTIAL_BACKOFF_RATE);
                log.debug("Throttled by service, retrying after {}ms.", sleepTimeMillis);
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
                long sleepTimeMillis = RetryUtils.calculateRetryBackoff(1, minRetryDelay.toMillis(), maxRetryDelay.toMillis(),
                        retry, 10, INLINE_EXPONENTIAL_BACKOFF_RATE);
                log.debug("Retryable client Exception, retrying after {}ms.", sleepTimeMillis);
                try {
                    Thread.sleep(sleepTimeMillis);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }
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
