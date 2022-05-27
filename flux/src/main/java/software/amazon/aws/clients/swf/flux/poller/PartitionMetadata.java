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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @JsonIgnore
    private static final Logger log = LoggerFactory.getLogger(PartitionMetadata.class);

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
        // Strictly speaking this doesn't need to be sorted but it makes testing a lot easier.
        this.partitionIds = Collections.unmodifiableSortedSet(new TreeSet<>(partitionIds));
        this.encodedAdditionalAttributes = Collections.unmodifiableMap(encodedAdditionalAttributes);
    }

    public static PartitionMetadata fromPartitionIdGeneratorResult(PartitionIdGeneratorResult result) {
        return new PartitionMetadata(result.getPartitionIds(),
                                     StepAttributes.serializeMapValues(result.getAdditionalAttributes()));
    }

    /**
     * Marker details can't exceed 32768 bytes, so we need to return a list of json-formatted strings with the partition id list
     * split between them.
     *
     * Only the first entry will contain the encodedAdditionalAttributes list.
     */
    @JsonIgnore
    public List<String> toMarkerDetailsList() throws JsonProcessingException {
        // Most of the time, the entire thing will fit in one marker, so we'll generate that and return it if it's short enough.
        // We do this because calculating the actual size one partition ID at a time is relatively expensive.

        // We'll leave ourselves a little wiggle room for our attribute names, and to simplify the logic below.
        // This way we can overrun our length limit by one partition id without it being a problem.
        // The actual max enforced by SWF is 32768, and partition IDs can't be longer than 250 characters anyway, since
        // the activity ID would get too long.
        int maxLength = 32000;

        String fullJson = MAPPER.writeValueAsString(this);
        if (fullJson.length() <= maxLength) {
            return Collections.singletonList(fullJson);
        }

        // The base json looks like this, which is 52 characters long:
        // {"partitionIds":[],"encodedAdditionalAttributes":{}}
        // We're going to account for the empty encodedAdditionalAttributes map below,
        // so we'll subtract two from the base length.
        int baseJsonSize = 50;

        Map<String, String> attributes = encodedAdditionalAttributes;
        int attributeMapSize = MAPPER.writeValueAsString(attributes).length();

        List<String> subsets = new LinkedList<>();

        // We'll use a TreeSet here even though it doesn't really need to be sorted in the output just so testing is easier.
        Set<String> partitionIdSubset = new TreeSet<>();
        int partitionIdSubsetSize = 0;
        for (String partitionId : partitionIds) {

            // This serialization includes the quotes around the partition IDs, and we need to account for a comma after each.
            // We can't just do a naive "length plus three"; there might be escaped characters in the serialized identifier.
            // This technically means we're counting one extra comma than we will actually use but this shouldn't cause
            // any extra markers to be generated since it only matters for the last partition ID, and we allow exceeding maxLength
            // for the last partition ID in each subset.
            String serialized = MAPPER.writeValueAsString(partitionId);
            partitionIdSubsetSize += serialized.length() + 1;

            // We add the partition ID here, not the serialized version, since we'll properly re-serialize the partition metadata
            // as a whole once we have enough partition IDs.
            partitionIdSubset.add(partitionId);

            if (partitionIdSubsetSize + attributeMapSize + baseJsonSize >= maxLength) {
                PartitionMetadata subset = new PartitionMetadata(partitionIdSubset, attributes);
                subsets.add(MAPPER.writeValueAsString(subset));

                partitionIdSubset.clear();
                partitionIdSubsetSize = 0;

                // This ensures only the first metadata marker contains the attributes.
                attributes = Collections.emptyMap();

                // Even though the subsequent pages have no attributes, they still have
                // the empty attribute map entry, which is two characters.
                attributeMapSize = 2;

            }
        }

        if (partitionIdSubset.size() > 0) {
            PartitionMetadata subset = new PartitionMetadata(partitionIdSubset, attributes);
            subsets.add(MAPPER.writeValueAsString(subset));
        }

        return subsets;
    }

    /**
     * Since toMarkerDetailsList() may split the marker into multiple json blobs, we need to reconstruct the original metadata from
     * a list of json blobs, combining their partition id lists.
     *
     * If any of the markers contain invalid JSON, the marker is skipped. If the list is empty or contains only invalid markers,
     * then this returns null.
     */
    public static PartitionMetadata fromMarkerDetailsList(List<String> markerDetailsList) throws JsonProcessingException {
        Set<String> partitionIds = new HashSet<>();
        Map<String, String> encodedAdditionalAttributes = new HashMap<>();
        boolean atLeastOneValidMetadataMarker = false;

        for (String markerDetails : markerDetailsList) {
            try {
                PartitionMetadata metadata = MAPPER.readValue(markerDetails, PartitionMetadata.class);
                partitionIds.addAll(metadata.getPartitionIds());
                encodedAdditionalAttributes.putAll(metadata.getEncodedAdditionalAttributes());
                atLeastOneValidMetadataMarker = true;
            } catch (JsonProcessingException e) {
                log.warn("Failed to deserialize partition metadata marker details, skipping.", e);
            }
        }

        if (!atLeastOneValidMetadataMarker) {
            return null;
        }

        return new PartitionMetadata(partitionIds, encodedAdditionalAttributes);
    }
}
