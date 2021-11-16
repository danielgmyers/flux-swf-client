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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for working with threads.
 */
public final class ThreadUtils {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtils.class);

    private ThreadUtils() {}

    /**
     * This is a copy of java.util.concurrent.Executors.DefaultThreadFactory, but with the pool name overridden.
     */
    static class NamedThreadFactory implements ThreadFactory {
        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(final String poolName) {
            SecurityManager securityManager = System.getSecurityManager();
            group = (securityManager != null) ? securityManager.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = String.format("%s-%d-thread-", poolName, POOL_NUMBER.getAndIncrement());
        }

        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0);
            if (thread.isDaemon()) {
                thread.setDaemon(false);
            }
            if (thread.getPriority() != Thread.NORM_PRIORITY) {
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            return thread;
        }
    }

    /**
     * We wrap the Runnable in an exception swallower (which logs a warning), but the default uncaught
     * exception handler still calls printStackTrace() on the exception. This method creates a thread factory
     * that overrides this behavior.
     */
    public static ThreadFactory createStackTraceSuppressingThreadFactory(final String poolName) {
        return new ThreadFactory() {
            private final ThreadFactory backingThreadFactory = new NamedThreadFactory(poolName);
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
