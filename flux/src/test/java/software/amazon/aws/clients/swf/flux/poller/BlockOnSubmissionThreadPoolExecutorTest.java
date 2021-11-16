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

package software.amazon.aws.clients.swf.flux.poller;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockOnSubmissionThreadPoolExecutorTest {

    private BlockOnSubmissionThreadPoolExecutor executor;

    @Before
    public void setup() {
        executor = new BlockOnSubmissionThreadPoolExecutor(1, "executor");
    }

    @Test
    public void executeWaitsToCallSupplierUntilThreadIsFree() throws InterruptedException, ExecutionException {
        Duration delay = Duration.ofMillis(50);
        CompletableFuture<Long> invokedAtNanoTime = new CompletableFuture<>();
        Supplier<Runnable> supplier = () -> {
            invokedAtNanoTime.complete(System.nanoTime());
            return null;
        };

        long startNanoTime = System.nanoTime();
        // Add a Runnable that sleeps for long enough to ensure that the thread pool thread is in use
        // when we call the execute overload that takes a Supplier.
        executor.execute(() -> {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
            }
        });
        Assert.assertTrue("The first call to execute should not block.", System.nanoTime() - startNanoTime < delay.toNanos());

        executor.executeWhenCapacityAvailable(supplier);
        Assert.assertTrue("The supplier should not be executed until a thread pool thread is free.",
                          // which means the supplier should only be invoked after the expected delay.
                          invokedAtNanoTime.get() - startNanoTime > delay.toNanos());
    }

    @Test(timeout = 500)
    public void semaphoreIsReleasedWhenSupplierReturnsNull() {
        Supplier<Runnable> supplier = () -> null;

        executor.executeWhenCapacityAvailable(supplier);
        // If the call above did not release the semaphore then the call below will block indefinitely
        // (well, until the timeout configured on this junit test method).
        executor.execute(() -> {});
    }

    @Test(timeout = 500)
    public void executeRunnableReturnedBySupplier() throws InterruptedException, ExecutionException {
        Duration delay = Duration.ofMillis(50);
        CompletableFuture<Boolean> runnableInvoked = new CompletableFuture<>();
        Supplier<Runnable> supplier = () -> () -> {
            try {
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
            }
            runnableInvoked.complete(true);
        };

        executor.executeWhenCapacityAvailable(supplier);
        // The call above should return before execution of the Runnable that was returned by the Supplier.
        // The assertion below verifies that the Runnable is executed asynchronously without blocking the execute() call.
        Assert.assertFalse("The Runnable should not have finished executing yet.", runnableInvoked.isDone());

        // Note that the call to runnableInvoked.get() will block until the runnable completes the Future.
        // So if the runnable does not get invoked the test will timeout.
        Assert.assertTrue("The Runnable should have run.", runnableInvoked.get());
    }

}