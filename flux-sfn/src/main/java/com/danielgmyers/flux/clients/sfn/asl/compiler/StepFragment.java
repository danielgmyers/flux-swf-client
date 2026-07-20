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

import java.util.Collections;
import java.util.Map;

import com.danielgmyers.flux.clients.sfn.asl.state.AslState;

/**
 * Represents a single Flux workflow step as a group of one or more ASL states.
 *
 * <p>A StepFragment is the intermediate representation (IR) produced by the compiler for each
 * {@link com.danielgmyers.flux.wf.graph.WorkflowGraphNode}. It has a well-defined entry point
 * (the state name that incoming transitions should target) and a set of exit transitions
 * (result codes that the assembler resolves to target the next fragment's entry state or a
 * terminal Succeed state).</p>
 *
 * <h2>ASL State Naming Convention</h2>
 *
 * <p>State names within a fragment follow the pattern:</p>
 * <pre>
 *   {WorkflowName}.{StepName}.{Suffix}
 * </pre>
 *
 * <p>The primary Task state (step execution) uses no suffix, i.e. just {@code WorkflowName.StepName}.
 * Internal states use a dot-separated suffix of at most 10 characters, fitting within the 80-character
 * ASL state name limit (34 workflow + 1 dot + 34 step + 1 dot + 10 suffix = 80).</p>
 *
 * <p>Defined suffixes:</p>
 * <table>
 *   <tr><th>Suffix</th><th>Length</th><th>Purpose</th></tr>
 *   <tr><td>(none)</td><td>0</td><td>Primary Task state (step execution)</td></tr>
 *   <tr><td>{@code Route}</td><td>5</td><td>Choice state for result code routing</td></tr>
 *   <tr><td>{@code BadResult}</td><td>9</td><td>Fail state for unrecognized result code</td></tr>
 *   <tr><td>{@code GenParts}</td><td>8</td><td>Task state for partition ID generation</td></tr>
 *   <tr><td>{@code MapParts}</td><td>8</td><td>Map state for partition execution</td></tr>
 *   <tr><td>{@code Partition}</td><td>9</td><td>Task state inside the Map's ItemProcessor</td></tr>
 * </table>
 *
 * <p>Additionally, a single shared terminal state is created per state machine:</p>
 * <pre>
 *   {WorkflowName}.Succeed
 * </pre>
 *
 * <h3>Examples</h3>
 *
 * <p>Standard step with branching (non-{@code _always}):</p>
 * <pre>
 *   OrderFlow.ValidateItem              (Task - execute step)
 *   OrderFlow.ValidateItem.Route        (Choice - route by result code)
 *   OrderFlow.ValidateItem.BadResult    (Fail - unrecognized result code)
 * </pre>
 *
 * <p>Step with {@code _always} transition:</p>
 * <pre>
 *   OrderFlow.ValidateItem              (Task - execute step, Next points directly to next fragment)
 * </pre>
 *
 * <p>Partitioned step:</p>
 * <pre>
 *   OrderFlow.ShipItems.GenParts        (Task - generate partition IDs)
 *   OrderFlow.ShipItems.MapParts        (Map - iterate partitions)
 *   OrderFlow.ShipItems.Partition       (Task - inside Map's ItemProcessor)
 *   OrderFlow.ShipItems.Route           (Choice - aggregate result routing)
 *   OrderFlow.ShipItems.BadResult       (Fail - unrecognized result code)
 * </pre>
 */
public class StepFragment {

    /**
     * Suffix for the Choice state that routes based on step result codes.
     */
    public static final String SUFFIX_ROUTE = "Route";

    /**
     * Suffix for the Fail state that handles unrecognized result codes.
     */
    public static final String SUFFIX_BAD_RESULT = "BadResult";

    /**
     * Suffix for the Task state that generates partition IDs.
     */
    public static final String SUFFIX_GENERATE_PARTITIONS = "GenParts";

    /**
     * Suffix for the Map state that executes partitions.
     */
    public static final String SUFFIX_MAP_PARTITIONS = "MapParts";

    /**
     * Suffix for the Task state inside the Map's ItemProcessor.
     */
    public static final String SUFFIX_PARTITION = "Partition";

    /**
     * Suffix for the shared workflow-level terminal Succeed state.
     * Used as {@code WorkflowName.Succeed} (no step name component).
     */
    public static final String SUFFIX_SUCCEED = "Succeed";

    private final String entryStateName;
    private final Map<String, AslState> states;
    private final Map<String, String> exitTransitionResultCodes;

    /**
     * Creates a new StepFragment.
     *
     * @param entryStateName The name of the ASL state that incoming transitions should target.
     * @param states The ASL states that make up this fragment, keyed by state name.
     *               These are merged into the final StateMachineDefinition's States map.
     * @param exitTransitionResultCodes A map from Flux result code to the ASL state name whose
     *                                  Next field should be resolved to the target fragment's entry.
     *                                  For {@code _always} steps, this contains a single entry with
     *                                  key {@link com.danielgmyers.flux.step.StepResult#ALWAYS_RESULT_CODE}.
     *                                  A null value indicates the transition closes the workflow
     *                                  (resolves to the shared Succeed state).
     */
    public StepFragment(String entryStateName, Map<String, AslState> states,
                        Map<String, String> exitTransitionResultCodes) {
        this.entryStateName = entryStateName;
        this.states = Collections.unmodifiableMap(states);
        this.exitTransitionResultCodes = Collections.unmodifiableMap(exitTransitionResultCodes);
    }

    /**
     * Returns the name of the ASL state that incoming transitions should point to.
     */
    public String getEntryStateName() {
        return entryStateName;
    }

    /**
     * Returns the ASL states that make up this fragment, keyed by state name.
     */
    public Map<String, AslState> getStates() {
        return states;
    }

    /**
     * Returns the exit transition map.
     *
     * <p>Keys are Flux result codes (e.g. {@code _succeed}, {@code _fail}, custom codes, or {@code _always}).
     * Values are the ASL state names within this fragment whose {@code Next} field the assembler
     * should resolve to point to the target fragment's entry state (or the shared Succeed state
     * if the transition closes the workflow).</p>
     *
     * <p>For {@code _always} steps, the value is the entry Task state name itself (its Next field
     * gets resolved directly). For branching steps, the values are the Choice rule Next fields
     * that need resolution.</p>
     */
    public Map<String, String> getExitTransitionResultCodes() {
        return exitTransitionResultCodes;
    }
}
