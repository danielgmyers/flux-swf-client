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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.aws.clients.swf.flux.step.PartitionIdGeneratorResult;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;

/**
 * Utility class for reading and writing the data in partition metadata markers.
 *
 * Package-private for access in tests.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final class PartitionMetadata {

    @JsonIgnore
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<String> partitionIds;
    private final Map<String, String> encodedAdditionalAttributes;

    public Set<String> getPartitionIds() {
        return partitionIds;
    }

    public Map<String, String> getEncodedAdditionalAttributes() {
        return encodedAdditionalAttributes;
    }

    @JsonCreator
    PartitionMetadata(@JsonProperty("partitionIds") Set<String> partitionIds,
                      @JsonProperty("encodedAdditionalAttributes") Map<String, String> encodedAdditionalAttributes) {
        this.partitionIds = Collections.unmodifiableSet(partitionIds);
        this.encodedAdditionalAttributes = Collections.unmodifiableMap(encodedAdditionalAttributes);
    }

    public static PartitionMetadata fromPartitionIdGeneratorResult(PartitionIdGeneratorResult result) {
        return new PartitionMetadata(result.getPartitionIds(),
                                     StepAttributes.serializeMapValues(result.getAdditionalAttributes()));
    }

    @JsonIgnore
    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }

    public static PartitionMetadata fromJson(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, PartitionMetadata.class);
    }
}
