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

package software.amazon.aws.clients.swf.flux.wf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * This annotation may be attached to an object that implements the Workflow interface.
 * If present, it indicates that the associated Workflow should be automatically run
 * by Flux no more often than the specified frequency. If the workflow ends earlier than that,
 * Flux will automatically insert a step at the end of the workflow which delays the exit
 * of that workflow until the specified interval has elapsed, at which point the workflow
 * will succeed and the next execution of the same workflow will be initiated. If the workflow takes longer than that
 * interval to complete, Flux will start the next execution one second later.
 *
 * Note that any combination of interval+unit that evaluates to less than 1 second will be treated as 1 second.
 *
 * The minimum timer granularity in SWF is 1 second; run intervals of 5 seconds or less should be avoided.
 * Instead, use a periodic workflow that specifies a relatively short run interval (e.g. 10 seconds),
 * and then implement a workflow step that sleep-loops more frequently. The main downside to this approach
 * is that it permanently locks a worker thread to just that task, so capacity planning should take that into account.
 *
 * When the next execution of the workflow is scheduled, Flux uses the ContinueAsNewWorkflowExecution operation,
 * which avoids impacting the StartWorkflowExecution rate limit.
 *
 * In order to ensure that new workflows are started after their first deployment, and to handle the case where
 * a workflow has been manually terminated, Flux will also call StartWorkflowExecution on each worker host at a
 * rate equal to half of each workflow's configured run interval. If a workflow's run interval is 10 minutes,
 * each of your worker hosts will make one StartWorkflowExecution call every 5 minutes for that workflow.
 * (This execution interval is clamped to a maximum rate of once per 5 seconds and a minimum rate of once per hour.)
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Periodic {

    /**
     * Allows the default run interval to be overridden.
     */
    int runInterval() default 1;

    /**
     * Allows the default run interval to be overridden.
     */
    TimeUnit intervalUnits() default TimeUnit.MINUTES;
}
