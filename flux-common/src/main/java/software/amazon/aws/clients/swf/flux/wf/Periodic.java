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
 * by Flux (at most) at a specified frequency. If the workflow ends earlier than that,
 * Flux will automatically insert a step at the end of the workflow which delays the exit
 * of that workflow until the specified interval has elapsed, at which point the workflow
 * will succeed and the next execution of the same workflow will be initiated.
 *
 * Note that any combination of interval+unit that evaluates to less than 1 second will be treated as 1 second.
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
