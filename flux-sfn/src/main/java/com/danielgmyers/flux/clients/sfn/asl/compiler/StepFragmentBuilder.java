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

package com.danielgmyers.flux.clients.sfn.asl.compiler;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.sfn.asl.state.AslState;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceRule;
import com.danielgmyers.flux.clients.sfn.asl.state.ChoiceState;
import com.danielgmyers.flux.clients.sfn.asl.state.FailState;
import com.danielgmyers.flux.clients.sfn.asl.state.MapState;
import com.danielgmyers.flux.clients.sfn.asl.state.Retrier;
import com.danielgmyers.flux.clients.sfn.asl.state.TaskState;
import com.danielgmyers.flux.clients.sfn.step.SfnStepInputAccessor;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.PartitionedWorkflowStep;
import com.danielgmyers.flux.step.StepApply;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.StepResult;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.step.internal.WorkflowStepUtil;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;

/**
 * Builds a {@link StepFragment} from a {@link WorkflowGraphNode}.
 *
 * <p>Examines the node's step type (regular vs. partitioned), its transition map (to determine
 * whether an {@code _always} or branching fragment is needed), and the step's {@link StepApply}
 * annotation (to extract retry configuration).</p>
 */
public class StepFragmentBuilder {

    /**
     * The maximum number of retry attempts before the execution fails.
     * Derived from SFN's 25,000 event history limit: each retry consumes ~6 events,
     * so (25000 / 6) ≈ 4166. We use 4000 as a conservative cap.
     */
    static final int MAX_RETRY_ATTEMPTS = 4000;

    /**
     * The default exponential backoff base, matching FluxCapacitorConfig's default.
     */
    static final double DEFAULT_EXPONENTIAL_BACKOFF_BASE = 1.25;

    /**
     * The error code used in SendTaskFailure for application-level retries.
     * Both exceptions and StepResult.retry() are reported as task failures with this error.
     */
    static final String TASK_FAILURE_ERROR_CODE = "States.TaskFailed";

    /**
     * The field name in the Task output JSON that carries the Flux result code.
     */
    static final String RESULT_CODE_JSON_PATH = "$._flux_resultCode";

    /**
     * The key used in the Task state's Parameters field to wrap all step attributes.
     * References {@link SfnStepInputAccessor#ATTRS_KEY} to ensure the compiler and executor agree
     * on the wrapping key. The {@code .$} suffix indicates this is a JSONPath reference in ASL Parameters.
     */
    static final String PARAMETERS_ATTRS_KEY = SfnStepInputAccessor.ATTRS_KEY + ".$";

    /**
     * The JSONPath value for the attributes pass-through in Parameters.
     * Passes the entire state input as-is into the _attrs sub-object.
     */
    static final String PARAMETERS_ATTRS_VALUE = "$";

    /**
     * The key used in the Task state's Parameters field to inject the retry count.
     * The trailing .$ indicates it's a JSONPath reference into the context object.
     */
    static final String PARAMETERS_RETRY_ATTEMPT_KEY = StepAttributes.RETRY_ATTEMPT + ".$";

    /**
     * The JSONPath context object reference for the retry count.
     */
    static final String PARAMETERS_RETRY_ATTEMPT_VALUE = "$$.State.RetryCount";

    /**
     * The key used in the Task state's Parameters field to inject the workflow execution ID.
     */
    static final String PARAMETERS_EXECUTION_ID_KEY = StepAttributes.WORKFLOW_EXECUTION_ID + ".$";

    /**
     * The JSONPath context object reference for the execution ID (ARN).
     */
    static final String PARAMETERS_EXECUTION_ID_VALUE = "$$.Execution.Id";

    /**
     * The key used in the Task state's Parameters field to inject the execution start time.
     */
    static final String PARAMETERS_WORKFLOW_START_TIME_KEY = StepAttributes.WORKFLOW_START_TIME + ".$";

    /**
     * The JSONPath context object reference for the execution start time.
     */
    static final String PARAMETERS_WORKFLOW_START_TIME_VALUE = "$$.Execution.StartTime";

    /**
     * The key used in the Task state's Parameters field to inject the user-supplied workflow ID.
     * In SFN, this corresponds to the execution name provided when starting the execution.
     */
    static final String PARAMETERS_WORKFLOW_ID_KEY = StepAttributes.WORKFLOW_ID + ".$";

    /**
     * The JSONPath context object reference for the execution name (user-supplied workflow ID).
     */
    static final String PARAMETERS_WORKFLOW_ID_VALUE = "$$.Execution.Name";

    /**
     * The key used in the Task state's Parameters field to inject the time the state was first entered.
     * This serves as a proxy for the initial attempt time — it does not reset across retries of the same state.
     */
    static final String PARAMETERS_INITIAL_ATTEMPT_TIME_KEY = StepAttributes.ACTIVITY_INITIAL_ATTEMPT_TIME + ".$";

    /**
     * The JSONPath context object reference for when the current state was first entered.
     */
    static final String PARAMETERS_INITIAL_ATTEMPT_TIME_VALUE = "$$.State.EnteredTime";

    /**
     * The key in the GenParts activity output that holds the array of partition IDs.
     * The Map state's ItemsPath references this field.
     */
    static final String PARTITION_IDS_FIELD = "_flux_partitionIds";

    /**
     * The intermediate key used by the Map state's ItemSelector to pass the outer attribute bag
     * into each iteration, and by the partition Task's Parameters to read it back out.
     */
    static final String STEP_INPUT_ATTRS_KEY = "_step_input_attrs";

    /**
     * The key used in the Map state's ItemSelector to pass the outer attribute bag into each iteration.
     */
    static final String ITEM_SELECTOR_ATTRS_KEY = STEP_INPUT_ATTRS_KEY + ".$";

    /**
     * The JSONPath value for the ItemSelector attrs pass-through — the Map state's effective input.
     */
    static final String ITEM_SELECTOR_ATTRS_VALUE = "$";

    /**
     * The key used in the Map state's ItemSelector to inject the partition ID from the current item.
     */
    static final String ITEM_SELECTOR_PARTITION_ID_KEY = StepAttributes.PARTITION_ID + ".$";

    /**
     * The JSONPath context object reference for the current Map iteration's value.
     */
    static final String ITEM_SELECTOR_PARTITION_ID_VALUE = "$$.Map.Item.Value";

    /**
     * The key used in the Map state's ItemSelector to inject the partition count.
     */
    static final String ITEM_SELECTOR_PARTITION_COUNT_KEY = StepAttributes.PARTITION_COUNT + ".$";

    /**
     * The JSONPath reference for the partition count field from the GenParts output.
     */
    static final String ITEM_SELECTOR_PARTITION_COUNT_VALUE = "$." + StepAttributes.PARTITION_COUNT;

    /**
     * The JSONPath used by the partition Task's Parameters to extract attributes
     * from the ItemSelector-structured iteration input.
     */
    static final String PARTITION_TASK_ATTRS_SOURCE = "$." + STEP_INPUT_ATTRS_KEY;

    private final Class<? extends Workflow> workflowClass;
    private final FluxCapacitorConfig config;

    public StepFragmentBuilder(Class<? extends Workflow> workflowClass, FluxCapacitorConfig config) {
        this.workflowClass = workflowClass;
        this.config = config;
    }

    /**
     * Builds a StepFragment for the given workflow graph node.
     */
    public StepFragment build(WorkflowGraphNode node) {
        WorkflowStep step = node.getStep();
        String stepName = TaskNaming.stepName(step);

        if (step instanceof PartitionedWorkflowStep) {
            return buildPartitionedFragment(node, stepName);
        } else {
            return buildStandardFragment(node, stepName);
        }
    }

    private StepFragment buildStandardFragment(WorkflowGraphNode node, String stepName) {
        Map<String, WorkflowGraphNode> transitions = node.getNextStepsByResultCode();
        String taskStateName = formatStateName(stepName, null);

        TaskState taskState = buildTaskState(node.getStep());

        boolean isAlwaysTransition = transitions.containsKey(StepResult.ALWAYS_RESULT_CODE);

        if (isAlwaysTransition) {
            // _always: Task state alone, Next resolved by assembler
            Map<String, AslState> states = new LinkedHashMap<>();
            states.put(taskStateName, taskState);

            Map<String, String> exitTransitions = new LinkedHashMap<>();
            exitTransitions.put(StepResult.ALWAYS_RESULT_CODE, taskStateName);

            return new StepFragment(taskStateName, states, exitTransitions);
        } else {
            // Branching: Task → Choice → (per result code)
            return buildBranchingFragment(taskState, taskStateName, stepName, transitions);
        }
    }

    private StepFragment buildBranchingFragment(TaskState taskState, String taskStateName, String stepName,
                                                Map<String, WorkflowGraphNode> transitions) {
        final String routeStateName = formatStateName(stepName, StepFragment.SUFFIX_ROUTE);
        final String badResultStateName = formatStateName(stepName, StepFragment.SUFFIX_BAD_RESULT);

        // Task's Next points to the Choice state
        taskState.setNext(routeStateName);

        // Build Choice state
        ChoiceState choiceState = new ChoiceState();
        List<ChoiceRule> rules = new ArrayList<>();
        Map<String, String> exitTransitions = new LinkedHashMap<>();

        for (String resultCode : transitions.keySet()) {
            ChoiceRule rule = new ChoiceRule();
            rule.setVariable(RESULT_CODE_JSON_PATH);
            rule.setStringEquals(resultCode);
            // Next will be resolved by the assembler; for now use a placeholder name
            // that identifies which result code this rule corresponds to.
            // The assembler knows to look at the Choice rules' Next fields.
            rule.setNext(resultCode);
            rules.add(rule);

            exitTransitions.put(resultCode, routeStateName);
        }

        choiceState.setChoices(rules);

        // Default → BadResult (unrecognized result code)
        // TODO: Replace with wait-for-human-input recovery mechanism
        // once external data store is available.
        choiceState.setDefaultState(badResultStateName);

        // Build Fail state for unrecognized result codes
        FailState failState = new FailState();
        failState.setError("Flux.UnrecognizedResultCode");
        failState.setCause("The step returned a result code that has no defined transition in the workflow graph.");

        // Assemble states map
        Map<String, AslState> states = new LinkedHashMap<>();
        states.put(taskStateName, taskState);
        states.put(routeStateName, choiceState);
        states.put(badResultStateName, failState);

        return new StepFragment(taskStateName, states, exitTransitions);
    }

    private StepFragment buildPartitionedFragment(WorkflowGraphNode node, String stepName) {
        final Map<String, WorkflowGraphNode> transitions = node.getNextStepsByResultCode();
        final String genPartsStateName = formatStateName(stepName, StepFragment.SUFFIX_GENERATE_PARTITIONS);
        final String mapPartsStateName = formatStateName(stepName, StepFragment.SUFFIX_MAP_PARTITIONS);
        final String partitionStateName = formatStateName(stepName, StepFragment.SUFFIX_PARTITION);
        final String routeStateName = formatStateName(stepName, StepFragment.SUFFIX_ROUTE);
        final String badResultStateName = formatStateName(stepName, StepFragment.SUFFIX_BAD_RESULT);

        // 1. Task state for partition ID generation
        TaskState genPartsTask = new TaskState();
        genPartsTask.setResource(formatActivityArn(node.getStep().getClass(), StepFragment.SUFFIX_GENERATE_PARTITIONS));
        genPartsTask.setRetry(buildRetryConfig(node.getStep()));
        genPartsTask.setParameters(buildTaskParameters());
        genPartsTask.setNext(mapPartsStateName);

        // 2. Map state for partition execution
        MapState mapState = new MapState();
        mapState.setItemsPath("$." + PARTITION_IDS_FIELD);
        mapState.setMaxConcurrency(0); // unbounded parallelism
        // Discard Map output — partitioned steps cannot add output attributes.
        // The Map's input (the attribute bag) passes through unchanged to the next state.
        mapState.setResultPath(null);
        // ItemSelector structures each iteration's input with the attribute bag,
        // partition ID, and partition count.
        mapState.setItemSelector(buildItemSelector());

        // Inner Task for each partition
        TaskState partitionTask = new TaskState();
        partitionTask.setResource(formatActivityArn(node.getStep().getClass(), null));
        partitionTask.setRetry(buildRetryConfig(node.getStep()));
        partitionTask.setParameters(buildPartitionTaskParameters());
        partitionTask.setEnd(true);

        MapState.ItemProcessor processor = new MapState.ItemProcessor();
        processor.setStartAt(partitionStateName);
        Map<String, AslState> processorStates = new LinkedHashMap<>();
        processorStates.put(partitionStateName, partitionTask);
        processor.setStates(processorStates);
        mapState.setItemProcessor(processor);

        // Map state transitions to the route (or directly if _always)
        boolean isAlwaysTransition = transitions.containsKey(StepResult.ALWAYS_RESULT_CODE);

        Map<String, AslState> states = new LinkedHashMap<>();
        states.put(genPartsStateName, genPartsTask);
        states.put(mapPartsStateName, mapState);

        Map<String, String> exitTransitions = new LinkedHashMap<>();

        if (isAlwaysTransition) {
            exitTransitions.put(StepResult.ALWAYS_RESULT_CODE, mapPartsStateName);
            return new StepFragment(genPartsStateName, states, exitTransitions);
        }

        // Branching: Map → Choice
        mapState.setNext(routeStateName);

        ChoiceState choiceState = new ChoiceState();
        List<ChoiceRule> rules = new ArrayList<>();

        for (String resultCode : transitions.keySet()) {
            ChoiceRule rule = new ChoiceRule();
            rule.setVariable(RESULT_CODE_JSON_PATH);
            rule.setStringEquals(resultCode);
            rule.setNext(resultCode);
            rules.add(rule);

            exitTransitions.put(resultCode, routeStateName);
        }

        choiceState.setChoices(rules);
        choiceState.setDefaultState(badResultStateName);

        // Fail state for unrecognized result codes
        FailState failState = new FailState();
        failState.setError("Flux.UnrecognizedResultCode");
        failState.setCause("The partitioned step returned a result code that has no defined transition in the workflow graph.");

        states.put(routeStateName, choiceState);
        states.put(badResultStateName, failState);

        return new StepFragment(genPartsStateName, states, exitTransitions);
    }

    /**
     * Builds a TaskState with the Activity resource ARN and retry configuration
     * derived from the step's @StepApply annotation.
     */
    private TaskState buildTaskState(WorkflowStep step) {
        TaskState taskState = new TaskState();
        taskState.setResource(formatActivityArn(step.getClass(), null));
        taskState.setRetry(buildRetryConfig(step));
        taskState.setParameters(buildTaskParameters());
        return taskState;
    }

    /**
     * Builds the Parameters map for a Task state.
     *
     * <p>This wraps the state input into a nested {@code _attrs} object and injects
     * context object values (retry count, execution ID, start time, state name) at the top level.
     * The activity executor unwraps {@code _attrs} and merges the injected values into the
     * step input before executing the step.</p>
     *
     * <p>On the output side, the activity executor emits a flat map of all attributes so that
     * subsequent states receive a clean top-level attribute map (avoiding nested wrapping
     * across multiple steps).</p>
     */
    private Map<String, Object> buildTaskParameters() {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put(PARAMETERS_ATTRS_KEY, PARAMETERS_ATTRS_VALUE);
        parameters.put(PARAMETERS_RETRY_ATTEMPT_KEY, PARAMETERS_RETRY_ATTEMPT_VALUE);
        parameters.put(PARAMETERS_EXECUTION_ID_KEY, PARAMETERS_EXECUTION_ID_VALUE);
        parameters.put(PARAMETERS_WORKFLOW_START_TIME_KEY, PARAMETERS_WORKFLOW_START_TIME_VALUE);
        parameters.put(PARAMETERS_WORKFLOW_ID_KEY, PARAMETERS_WORKFLOW_ID_VALUE);
        parameters.put(PARAMETERS_INITIAL_ATTEMPT_TIME_KEY, PARAMETERS_INITIAL_ATTEMPT_TIME_VALUE);
        return parameters;
    }

    /**
     * Builds the ItemSelector map for the Map state in a partitioned step fragment.
     *
     * <p>Structures each iteration's input to contain:</p>
     * <ul>
     *   <li>{@code _step_input_attrs} — the full attribute bag from the Map state's input</li>
     *   <li>{@code _partition_id} — the current iteration's partition ID</li>
     *   <li>{@code _partition_count} — the total number of partitions</li>
     * </ul>
     */
    private Map<String, Object> buildItemSelector() {
        Map<String, Object> itemSelector = new LinkedHashMap<>();
        itemSelector.put(ITEM_SELECTOR_ATTRS_KEY, ITEM_SELECTOR_ATTRS_VALUE);
        itemSelector.put(ITEM_SELECTOR_PARTITION_ID_KEY, ITEM_SELECTOR_PARTITION_ID_VALUE);
        itemSelector.put(ITEM_SELECTOR_PARTITION_COUNT_KEY, ITEM_SELECTOR_PARTITION_COUNT_VALUE);
        return itemSelector;
    }

    /**
     * Builds the Parameters map for the inner partition Task state.
     *
     * <p>Unlike {@link #buildTaskParameters()}, this reads the attribute bag from
     * {@code $._step_input_attrs} (produced by the Map state's ItemSelector) and
     * passes through the partition ID and count from the iteration input.</p>
     */
    private Map<String, Object> buildPartitionTaskParameters() {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put(PARAMETERS_ATTRS_KEY, PARTITION_TASK_ATTRS_SOURCE);
        parameters.put(StepAttributes.PARTITION_ID + ".$", "$." + StepAttributes.PARTITION_ID);
        parameters.put(StepAttributes.PARTITION_COUNT + ".$", "$." + StepAttributes.PARTITION_COUNT);
        parameters.put(PARAMETERS_RETRY_ATTEMPT_KEY, PARAMETERS_RETRY_ATTEMPT_VALUE);
        parameters.put(PARAMETERS_EXECUTION_ID_KEY, PARAMETERS_EXECUTION_ID_VALUE);
        parameters.put(PARAMETERS_WORKFLOW_START_TIME_KEY, PARAMETERS_WORKFLOW_START_TIME_VALUE);
        parameters.put(PARAMETERS_WORKFLOW_ID_KEY, PARAMETERS_WORKFLOW_ID_VALUE);
        parameters.put(PARAMETERS_INITIAL_ATTEMPT_TIME_KEY, PARAMETERS_INITIAL_ATTEMPT_TIME_VALUE);
        return parameters;
    }

    /**
     * Builds the Retry configuration for a Task state from the step's @StepApply annotation.
     *
     * <p>Produces up to two Retriers:</p>
     * <ol>
     *   <li>Flat-rate retries (before backoff): uses BackoffRate 1.0 for {@code retriesBeforeBackoff} attempts.
     *       Omitted if {@code retriesBeforeBackoff} is less than or equal to 1.</li>
     *   <li>Exponential backoff retries: uses the configured backoff base, capped at {@link #MAX_RETRY_ATTEMPTS}.
     *       Omitted if {@code maxRetryDelaySeconds} equals {@code initialRetryDelaySeconds} (no actual backoff).</li>
     * </ol>
     *
     * <p>At least one Retrier is always produced. If both conditions for omission are met, the flat-rate retrier
     * is emitted with {@link #MAX_RETRY_ATTEMPTS} as its max attempts.</p>
     */
    List<Retrier> buildRetryConfig(WorkflowStep step) {
        Method applyMethod = WorkflowStepUtil.getUniqueAnnotatedMethod(step.getClass(), StepApply.class);
        StepApply applyConfig = applyMethod.getAnnotation(StepApply.class);

        final long initialDelay = applyConfig.initialRetryDelaySeconds();
        final long maxDelay = applyConfig.maxRetryDelaySeconds();
        final long retriesBeforeBackoff = applyConfig.retriesBeforeBackoff();
        final long jitterPercent = applyConfig.jitterPercent();
        final double backoffBase = applyConfig.exponentialBackoffBase() > 0.0
                ? applyConfig.exponentialBackoffBase()
                : (config.getExponentialBackoffBase() != null ? config.getExponentialBackoffBase()
                        : DEFAULT_EXPONENTIAL_BACKOFF_BASE);

        String jitterStrategy = jitterPercent > 0 ? "FULL" : null;

        final boolean includeBackoffRetrier = maxDelay != initialDelay;
        final boolean includeFlatRetrier = (retriesBeforeBackoff > 1) || !includeBackoffRetrier;

        List<Retrier> retriers = new ArrayList<>();

        // Retrier 1: flat-rate retries before backoff kicks in.
        // Omitted if retriesBeforeBackoff <= 1 and a backoff retrier is present.
        if (includeFlatRetrier) {
            Retrier flatRetrier = new Retrier();
            flatRetrier.setErrorEquals(List.of(TASK_FAILURE_ERROR_CODE));
            flatRetrier.setIntervalSeconds((int) initialDelay);
            flatRetrier.setMaxAttempts(includeBackoffRetrier ? (int) retriesBeforeBackoff : MAX_RETRY_ATTEMPTS);
            flatRetrier.setBackoffRate(1.0);
            if (jitterStrategy != null) {
                flatRetrier.setJitterStrategy(jitterStrategy);
            }
            retriers.add(flatRetrier);
        }

        // Retrier 2: exponential backoff.
        // Omitted if maxRetryDelaySeconds == initialRetryDelaySeconds (no actual backoff).
        if (includeBackoffRetrier) {
            Retrier backoffRetrier = new Retrier();
            backoffRetrier.setErrorEquals(List.of(TASK_FAILURE_ERROR_CODE));
            backoffRetrier.setIntervalSeconds((int) initialDelay);
            backoffRetrier.setMaxAttempts(MAX_RETRY_ATTEMPTS);
            backoffRetrier.setBackoffRate(backoffBase);
            backoffRetrier.setMaxDelaySeconds((int) maxDelay);
            if (jitterStrategy != null) {
                backoffRetrier.setJitterStrategy(jitterStrategy);
            }
            retriers.add(backoffRetrier);
        }

        return retriers;
    }

    /**
     * Formats an ASL state name following the naming convention:
     * {@code WorkflowName.StepName} or {@code WorkflowName.StepName.Suffix}.
     */
    String formatStateName(String stepName, String suffix) {
        String baseName = TaskNaming.workflowName(workflowClass) + "." + stepName;
        if (suffix == null) {
            return baseName;
        }
        return baseName + "." + suffix;
    }

    /**
     * Formats the Activity ARN for a step's Task state.
     * If suffix is non-null, it's appended to differentiate sub-activities (e.g. partition generation).
     */
    private String formatActivityArn(Class<? extends WorkflowStep> stepClass, String suffix) {
        if (suffix != null) {
            return SfnArnFormatter.activityArn(config.getAwsRegion(), config.getAwsAccountId(),
                                               workflowClass, stepClass, suffix);
        }
        return SfnArnFormatter.activityArn(config.getAwsRegion(), config.getAwsAccountId(),
                                           workflowClass, stepClass);
    }
}
