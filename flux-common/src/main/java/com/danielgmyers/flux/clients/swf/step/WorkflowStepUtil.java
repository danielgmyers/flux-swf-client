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

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.danielgmyers.flux.clients.swf.metrics.MetricRecorder;
import com.danielgmyers.flux.clients.swf.metrics.MetricRecorderFactory;
import com.danielgmyers.flux.clients.swf.poller.TaskNaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for extracting data about a workflow step.
 */
public final class WorkflowStepUtil {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStepUtil.class);

    private WorkflowStepUtil() {}

    /**
     * Given a type, finds the method that has the specified annotation.
     * Throws an exception if the type does not have exactly one method with that annotation.
     */
    public static Method getUniqueAnnotatedMethod(Class<?> clazz, Class<? extends Annotation> annotationType) {
        Method annotatedMethod = null;
        for (Method method : clazz.getMethods()) {
            if (method.isAnnotationPresent(annotationType)) {
                if (annotatedMethod != null) {
                    String message = String.format("Class %s must not have more than one @%s method.",
                                                   clazz.getSimpleName(), annotationType.getSimpleName());
                    log.error(message);
                    throw new RuntimeException(message);
                }
                annotatedMethod = method;
            }
        }
        if (annotatedMethod == null) {
            String message = String.format("Class %s must have a @%s method.", clazz.getSimpleName(),
                                           annotationType.getSimpleName());
            log.error(message);
            throw new RuntimeException(message);
        }
        log.debug("Found @{} method {}.{}", annotationType.getSimpleName(), clazz.getSimpleName(), annotatedMethod.getName());
        return annotatedMethod;
    }

    /**
     * Given a type, finds all methods that have the specified annotation and maps those methods to the specified annotation.
     */
    public static <T extends Annotation> Map<Method, T> getAllMethodsWithAnnotation(Class<?> clazz, Class<T> annotationType) {
        Map<Method, T> matching = new HashMap<>();
        for (Method method : clazz.getMethods()) {
            if (method.isAnnotationPresent(annotationType)) {
                matching.put(method, method.getAnnotation(annotationType));
            }
        }
        if (matching.isEmpty()) {
            log.debug("No @{} methods found in {}.", annotationType.getSimpleName(), clazz.getSimpleName());
        } else {
            String names = matching.keySet().stream().map(Method::getName).collect(Collectors.joining(", "));
            log.debug("Found {} @{} methods in {}: {}", matching.size(), annotationType.getSimpleName(),
                                    clazz.getSimpleName(), names);
        }
        return matching;
    }

    /**
     * Locates the @PartitionIdGenerator annotated method on the provided PartitionedWorkflowStep,
     * and calls it with the appropriate arguments. Returns the resulting list of partition IDs.
     */
    public static PartitionIdGeneratorResult getPartitionIdsForPartitionedStep(PartitionedWorkflowStep step,
                                                                               Map<String, String> stepInput,
                                                                               String workflowName, String workflowId,
                                                                               MetricRecorderFactory metricsFactory) {
        String activityName = TaskNaming.activityName(workflowName, step);
        Method partitionIdMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), PartitionIdGenerator.class);
        try (MetricRecorder stepMetrics = metricsFactory.newMetricRecorder(activityName + "." + partitionIdMethod.getName())) {
            if (!PartitionIdGeneratorResult.class.equals(partitionIdMethod.getReturnType())) {
                // the return type of this method is validated by the workflow graph builder, so this shouldn't happen
                throw new RuntimeException(String.format("%s.%s must have return type PartitionIdGeneratorResult.",
                                                         step.getClass().getSimpleName(), partitionIdMethod.getName()));
            }

            Object[] arguments = WorkflowStepUtil.generateArguments(step.getClass(), partitionIdMethod, stepMetrics,
                                                                    stepInput);
            return (PartitionIdGeneratorResult)(partitionIdMethod.invoke(step, arguments));
        } catch (IllegalAccessException | InvocationTargetException e) {
            String message = "Got an exception while attempting to request partition ids for workflow " + workflowId
                                + " for step " + activityName;
            log.error(message, e);
            // throwing this exception should cause the decision task to fail and be rescheduled.
            throw new RuntimeException(message, e);
        }
    }

    /**
     * Given a type, the method being called, and the available input attributes,
     * generates an array of input parameters for the method.
     */
    public static Object[] generateArguments(Class<?> clazz, Method method, MetricRecorder metrics,
                                             Map<String, String> input) {
        Object[] args = new Object[method.getParameterCount()];

        int arg = 0;
        for (Parameter param : method.getParameters()) {
            if (param.getType().isAssignableFrom(MetricRecorder.class)) {
                args[arg] = metrics;
            } else {
                Attribute attr = param.getAnnotation(Attribute.class);
                if (attr == null || attr.value().equals("")) {
                    String message = String.format("The %s.%s parameter %s must have the @Attribute annotation"
                                                           + " and its value must not be blank.",
                                                   clazz.getSimpleName(),
                                                   method.getName(),
                                                   param.getName());
                    log.error(message);
                    throw new RuntimeException(message);
                }
                args[arg] = StepAttributes.decode(param.getType(), input.get(attr.value()));
            }
            arg++;
        }

        return args;
    }

    /**
     * Executes a set of step hooks with the specified input attributes, for any hooks whose type matches the specified hook type.
     * Returns null unless the step should be retried due to a hook failure.
     */
    public static StepResult executeHooks(List<WorkflowStepHook> hooks, Map<String, String> hookInput, StepHook.HookType hookType,
                                          String activityName, MetricRecorder fluxMetrics, MetricRecorder hookMetrics) {
        for (WorkflowStepHook hook : hooks) {
            Map<Method, StepHook> methods = WorkflowStepUtil.getAllMethodsWithAnnotation(hook.getClass(), StepHook.class);
            for (Map.Entry<Method, StepHook> method : methods.entrySet()) {
                if (method.getValue().hookType() != hookType) {
                    continue;
                }
                String hookExecutionTimeMetricName = formatHookExecutionTimeName(hook.getClass().getSimpleName(),
                                                        method.getKey().getName(), activityName);
                fluxMetrics.startDuration(hookExecutionTimeMetricName);
                try {
                    Object result = method.getKey().invoke(hook, WorkflowStepUtil.generateArguments(hook.getClass(),
                                                                                                    method.getKey(),
                                                                                                    hookMetrics,
                                                                                                    hookInput));
                    if (result != null) {
                        log.info("Hook {} for activity {} returned value: {}",
                                               hook.getClass().getSimpleName(), activityName, result);
                    }
                } catch (InvocationTargetException e) {
                    String message = String.format("Hook %s for activity %s threw an exception (%s)",
                                                   hook.getClass().getSimpleName(), activityName, e.getCause().toString());
                    if (method.getValue().retryOnFailure()) {
                        message += ", and the hook is configured to retry on failure.";
                        log.info(message, e.getCause());
                        return StepResult.retry(message);
                    } else {
                        log.info("{}, but the hook is configured to ignore failures.", message, e.getCause());
                    }
                } catch (IllegalAccessException e) {
                    // IllegalAccessException shouldn't happen, since we only looked for public methods, but we'll handle it
                    // the same way as if the hook itself threw an exception.
                    String message = String.format("Hook %s for activity %s threw an exception (%s)",
                                                   hook.getClass().getSimpleName(), activityName, e.toString());
                    if (method.getValue().retryOnFailure()) {
                        message += ", and the hook is configured to retry on failure.";
                        log.error(message, e.getCause());
                        return StepResult.retry(message);
                    } else {
                        log.error("{}, but the hook is configured to ignore failures.", message, e.getCause());
                    }
                } finally {
                    fluxMetrics.endDuration(hookExecutionTimeMetricName);
                }
            }
        }
        return null;
    }

    // public only for testing visibility
    public static String formatHookExecutionTimeName(String hookClassName, String hookMethodName, String activityName) {
        return String.format("Flux.HookExecutionTime.%s.%s:Activity.%s", hookClassName, hookMethodName, activityName);
    }
}
