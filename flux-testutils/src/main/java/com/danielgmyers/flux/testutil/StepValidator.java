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

package com.danielgmyers.flux.testutil;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.danielgmyers.flux.poller.ActivityExecutionUtil;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.StepResult.ResultAction;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.danielgmyers.metrics.recorders.NoopMetricRecorderFactory;

/**
 * Helper class for validating WorkflowStep apply/rollback behavior.
 */
public final class StepValidator {

    private static final MetricRecorderFactory METRICS_FACTORY = new NoopMetricRecorderFactory();

    private StepValidator() {}

    public static StepResult completes(WorkflowStep step, Map<String, Object> input, String resultCode) {
        return apply(step, input, resultCode, ResultAction.COMPLETE, METRICS_FACTORY.newMetricRecorder(""));
    }

    public static StepResult completes(WorkflowStep step, Map<String, Object> input, String resultCode,
                                       MetricRecorder stepMetrics) {
        return apply(step, input, resultCode, ResultAction.COMPLETE, stepMetrics);
    }

    public static StepResult succeeds(WorkflowStep step, Map<String, Object> input) {
        return apply(step, input, StepResult.SUCCEED_RESULT_CODE, ResultAction.COMPLETE,
                     METRICS_FACTORY.newMetricRecorder(""));
    }

    public static StepResult succeeds(WorkflowStep step, Map<String, Object> input, MetricRecorder stepMetrics) {
        return apply(step, input, StepResult.SUCCEED_RESULT_CODE, ResultAction.COMPLETE, stepMetrics);
    }

    public static StepResult fails(WorkflowStep step, Map<String, Object> input) {
        return apply(step, input, StepResult.FAIL_RESULT_CODE, ResultAction.COMPLETE,
                     METRICS_FACTORY.newMetricRecorder(""));
    }

    public static StepResult fails(WorkflowStep step, Map<String, Object> input, MetricRecorder stepMetrics) {
        return apply(step, input, StepResult.FAIL_RESULT_CODE, ResultAction.COMPLETE, stepMetrics);
    }

    public static StepResult retries(WorkflowStep step, Map<String, Object> input) {
        return apply(step, input, null, ResultAction.RETRY, METRICS_FACTORY.newMetricRecorder(""));
    }

    public static StepResult retries(WorkflowStep step, Map<String, Object> input, MetricRecorder stepMetrics) {
        return apply(step, input, null, ResultAction.RETRY, stepMetrics);
    }

    private static StepResult apply(WorkflowStep step, Map<String, Object> input, String resultCode,
                                    ResultAction expectedResult, MetricRecorder stepMetrics) {
        Map<String, Object> augmentedInput = new HashMap<>(input);

        augmentedInput.putIfAbsent(StepAttributes.WORKFLOW_ID, "some-workflow-id");
        augmentedInput.putIfAbsent(StepAttributes.WORKFLOW_EXECUTION_ID, UUID.randomUUID().toString());
        augmentedInput.putIfAbsent(StepAttributes.WORKFLOW_START_TIME, Date.from(Instant.now()));

        StepResult actual = ActivityExecutionUtil.executeActivity(step, step.getClass().getSimpleName(),
                                                                  METRICS_FACTORY.newMetricRecorder(""), stepMetrics,
                                                                  StepAttributes.serializeMapValues(augmentedInput));
        if (actual.getAction() != expectedResult) {
            throw new RuntimeException(String.format("Expected result action %s but was %s: %s",
                                                     expectedResult, actual.getAction(), actual.getMessage()),
                                       actual.getCause());
        }
        if (resultCode != null && !resultCode.equals(actual.getResultCode())) {
            throw new RuntimeException(String.format("Expected result code %s but was %s",
                                                     resultCode, actual.getResultCode()));
        }
        return actual;
    }

}
