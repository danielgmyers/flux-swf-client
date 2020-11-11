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

package software.amazon.aws.clients.swf.flux.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for working with threads.
 */
public final class ThreadUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);

    private ThreadUtils() {}

    /**
     * We wrap the Runnable in an exception swallower (which logs a warning), but the default uncaught
     * exception handler still calls printStackTrace() on the exception. This method creates a thread factory
     * that overrides this behavior.
     */
    public static ThreadFactory createStackTraceSuppressingThreadFactory() {
        return new ThreadFactory() {
            private final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = backingThreadFactory.newThread(runnable);
                // Intentionally do nothing. This exception will be logged by our exception-swallowing wrapper.
                thread.setUncaughtExceptionHandler((thread1, throwable) -> { });
                return thread;
            }
        };
    }

    /**
     * ScheduledExecutorService will stop scheduling a task if any instances of it throw an exception.
     * To avoid this we will wrap our stuff in a Runnable that swallows (and logs) any such exceptions.
     *
     * @param runnable The runnable to wrap
     * @return The wrapped runnable
     */
    public static Runnable wrapInExceptionSwallower(Runnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                log.warn("Got an error while running thread {}", Thread.currentThread().getName(), t);
            }
        };
    }
}
