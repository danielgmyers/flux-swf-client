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

package software.amazon.aws.clients.swf.flux.poller;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.util.ThreadUtils;

/**
 * This class enforces that any attempt to submit a task to the pool will only proceed if there is a free thread.
 * It does this using a semaphore, rather than trying to figure out how many active threads the pool reports.
 */
public class BlockOnSubmissionThreadPoolExecutor extends ThreadPoolExecutor {

    private final Logger log = LoggerFactory.getLogger(BlockOnSubmissionThreadPoolExecutor.class);

    private final Semaphore submissionSemaphore;

    /**
     * Creates a fixed-size thread pool.
     */
    public BlockOnSubmissionThreadPoolExecutor(int fixedPoolSize, final String poolName) {
        super(fixedPoolSize, fixedPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(fixedPoolSize),
              ThreadUtils.createStackTraceSuppressingThreadFactory(poolName));
        submissionSemaphore = new Semaphore(fixedPoolSize);
    }

    @Override
    public void execute(Runnable runnable) {
        submissionSemaphore.acquireUninterruptibly();
        super.execute(runnable);
    }

    /**
     * Blocks until a thread is free, then executes the Supplier on the current thread and schedules the Runnable
     * to execute in the thread pool.
     */
    public void executeWhenCapacityAvailable(Supplier<Runnable> supplier) {
        submissionSemaphore.acquireUninterruptibly();
        Runnable runnable = null;
        try {
            runnable = supplier.get();
        } finally {
            // If there was nothing to run then release the semaphore.
            // Otherwise execute it on the thread pool.
            if (runnable == null) {
                submissionSemaphore.release();
            } else {
                super.execute(runnable);
            }
        }
    }

    @Override
    protected void afterExecute(Runnable var1, Throwable var2) {
        // Technically this means the release happens before the thread is actually freed,
        // but the thread is *about* to end at this point, so it should be close enough.
        submissionSemaphore.release();
    }

}
