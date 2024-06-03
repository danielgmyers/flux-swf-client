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

package com.danielgmyers.flux.clients.swf.poller;

import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for writing the user-facing execution context.
 *
 * Package-private for access in tests.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final class ExecutionContextMetadata {

    static final String KEY_METADATA_VERSION = "fluxMetadataVersion";
    static final String KEY_NEXT_STEP_NAME = "_nextStepName";
    static final String KEY_NEXT_STEP_RESULT_CODES = "_nextStepSupportedResultCodes";
    static final String NEXT_STEP_RESULT_WORKFLOW_ENDS = "_closeWorkflow";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Description of the versions, just for future reference:
     * 1 - Contains next step name and result codes but no version field. Attribute values are double-encoded.
     * 2 - Values are now regular json string/map types, version field is explicitly included in the context.
     *
     * Note that since Flux never actually reads the execution context, this version is only provided for users
     * to have a way to check that their parsing code will use the right logic during version upgrades;
     * additionally, Flux doesn't need to know how to decode or handle previous versions.
     *
     * We should increment this if we rename an existing field or change its type or encoding format.
     */
    static final Long CURRENT_METADATA_VERSION = 2L;

    @JsonProperty(value = KEY_METADATA_VERSION)
    private Long metadataVersion = CURRENT_METADATA_VERSION;

    @JsonProperty(value = KEY_NEXT_STEP_NAME)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String nextStepName;

    @JsonProperty(value = KEY_NEXT_STEP_RESULT_CODES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, String> resultCodeMap;

    public Long getMetadataVersion() {
        return metadataVersion;
    }

    public String getNextStepName() {
        return nextStepName;
    }

    public void setNextStepName(String nextStepName) {
        this.nextStepName = nextStepName;
    }

    public Map<String, String> getResultCodeMap() {
        return resultCodeMap;
    }

    public void populateExecutionContext(Class<? extends WorkflowStep> nextStep, WorkflowGraph graph) {
        nextStepName = nextStep.getSimpleName();
        resultCodeMap = new HashMap<>();
        WorkflowGraphNode node = graph.getNodes().get(nextStep);
        for (Map.Entry<String, WorkflowGraphNode> entry : node.getNextStepsByResultCode().entrySet()) {
            if (entry.getValue() == null) {
                resultCodeMap.put(entry.getKey(), NEXT_STEP_RESULT_WORKFLOW_ENDS);
            } else {
                resultCodeMap.put(entry.getKey(), entry.getValue().getStep().getClass().getSimpleName());
            }
        }
    }

    /**
     * If nextStepName and resultCodeMap are empty/null, this returns null, since there's no point
     * populating context data in SWF if we don't have any.
     */
    public String encode() throws JsonProcessingException {
        if (nextStepName == null && (resultCodeMap == null || resultCodeMap.isEmpty())) {
            return null;
        }
        return MAPPER.writeValueAsString(this);
    }

    /**
     * Flux itself never needs to decode the execution context since it's provided for user convenience;
     * this method is only provided for testing purposes.
     */
    public static ExecutionContextMetadata decode(String encoded) throws JsonProcessingException {
        if (encoded == null) {
            return null;
        }
        return MAPPER.readValue(encoded, ExecutionContextMetadata.class);
    }
}
