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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.danielgmyers.flux.clients.swf.IdUtils;
import com.danielgmyers.flux.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.step.StepAttributes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionMetadataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testFromPartitionIdGeneratorResult_NoAttributes() {
        Set<String> partitionIds = new TreeSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());

        Assertions.assertEquals(Collections.emptyMap(), metadata.getEncodedAdditionalAttributes());

        Assertions.assertEquals(PartitionMetadata.CURRENT_METADATA_VERSION, metadata.getFluxMetadataVersion());
    }

    @Test
    public void testFromPartitionIdGeneratorResult_WithAttributes() {
        Set<String> partitionIds = new TreeSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Instant now = Instant.now();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("timestamp", now);
        attributes.put("yes", true);

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds).withAttributes(attributes);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());

        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);
        Assertions.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());

        Assertions.assertEquals(PartitionMetadata.CURRENT_METADATA_VERSION, metadata.getFluxMetadataVersion());
    }

    @Test
    public void testToMarkerDetailsList_FitsInOneMarker() throws JsonProcessingException {
        Set<String> partitionIds = new TreeSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds).withAttributes(attributes);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);

        String expectedJson = "{\"fluxMetadataVersion\":1,"
                              + "\"partitionIds\":[\"p1\",\"p2\",\"p3\"],"
                              + "\"encodedAdditionalAttributes\":"
                              +   "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}"
                              + "}";

        List<String> detailsList = metadata.toMarkerDetailsList();
        Assertions.assertEquals(1, detailsList.size());
        Assertions.assertEquals(expectedJson, detailsList.get(0));
    }

    @Test
    public void testToMarkerDetailsList_MultipleMarkers() throws JsonProcessingException {
        // We'll use a TreeSet so that the set is consistently sorted when we iterate through it.
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        List<String> expectedMarkers = generateExpectedMarkers(partitionIds, attributes);
        // with a thousand 100-character partition ids, there should be 4 pages. This is just a sanity check.
        Assertions.assertEquals(4, expectedMarkers.size());

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds).withAttributes(attributes);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);
        Assertions.assertNotNull(metadata);

        List<String> detailsList = metadata.toMarkerDetailsList();
        Assertions.assertEquals(expectedMarkers, detailsList);
    }

    @Test
    public void testFromMarkerDetailsList_NoMetadataVersionField() throws JsonProcessingException {
        String inputJson = "{\"partitionIds\":[\"p1\",\"p2\",\"p3\"],"
                + "\"encodedAdditionalAttributes\":"
                +   "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}"
                + "}";
        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(Collections.singletonList(inputJson));
        Assertions.assertNotNull(metadata);

        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
        Assertions.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());

        Assertions.assertEquals(1L, metadata.getFluxMetadataVersion());
    }

    @Test
    public void testFromMarkerDetailsList_OneMarker() throws JsonProcessingException {
        String inputJson = "{\"fluxMetadataVersion\":1,"
                              + "\"partitionIds\":[\"p1\",\"p2\",\"p3\"],"
                              + "\"encodedAdditionalAttributes\":"
                              +   "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}"
                              + "}";
        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(Collections.singletonList(inputJson));
        Assertions.assertNotNull(metadata);

        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
        Assertions.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());
    }

    @Test
    public void testFromMarkerDetailsList_InvalidMarker() throws JsonProcessingException {
        String inputJson = "this is not valid json {";
        Assertions.assertNull(PartitionMetadata.fromMarkerDetailsList(Collections.singletonList(inputJson)));
    }

    @Test
    public void testFromMarkerDetailsList_MultipleMarkers() throws JsonProcessingException {
        // We'll use a TreeSet so that the set is consistently sorted when we iterate through it.
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        List<String> generatedMarkers = generateExpectedMarkers(partitionIds, attributes);
        // with a thousand 100-character partition ids, there should be 4 pages. This is just a sanity check.
        Assertions.assertEquals(4, generatedMarkers.size());

        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(generatedMarkers);
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
        Assertions.assertEquals(StepAttributes.serializeMapValues(attributes), metadata.getEncodedAdditionalAttributes());

        Assertions.assertEquals(PartitionMetadata.CURRENT_METADATA_VERSION, metadata.getFluxMetadataVersion());
    }

    @Test
    public void testFromMarkerDetailsList_IgnoresInvalidExtraMarker() throws JsonProcessingException {
        // We'll use a TreeSet so that the set is consistently sorted when we iterate through it.
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        List<String> generatedMarkers = generateExpectedMarkers(partitionIds, attributes);
        // with a thousand 100-character partition ids, there should be 4 pages. This is just a sanity check.
        Assertions.assertEquals(4, generatedMarkers.size());

        List<String> inputMarkers = new ArrayList<>(generatedMarkers);
        // insert invalid json in the middle
        inputMarkers.add(2, "this is not valid json {");
        Assertions.assertEquals(5, inputMarkers.size());

        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(inputMarkers);
        Assertions.assertNotNull(metadata);

        Assertions.assertEquals(partitionIds, metadata.getPartitionIds());
        Assertions.assertEquals(StepAttributes.serializeMapValues(attributes), metadata.getEncodedAdditionalAttributes());

        Assertions.assertEquals(PartitionMetadata.CURRENT_METADATA_VERSION, metadata.getFluxMetadataVersion());
    }

    private List<String> generateExpectedMarkers(Set<String> partitionIds, Map<String, Object> attributes)
            throws JsonProcessingException {
        PartitionMetadata empty = PartitionMetadata.fromPartitionIdGeneratorResult(PartitionIdGeneratorResult.create());
        String emptyEncodedMarker = empty.toMarkerDetailsList().get(0);

        PartitionIdGeneratorResult resultWithAttributes = PartitionIdGeneratorResult.create().withAttributes(attributes);
        PartitionMetadata emptyWithAttributes = PartitionMetadata.fromPartitionIdGeneratorResult(resultWithAttributes);
        String emptyWithAttributesEncodedMarker = emptyWithAttributes.toMarkerDetailsList().get(0);

        // Each partition ID has double-quotes around it and the IDs are comma-separated.
        // So a list of N 100-character partition IDs will use up N*100 + N*2 + (N-1) characters, or (N*103)-1.
        // To determine how many partition IDs are on the first page, we need to solve this equation for N:
        // 32000 - emptyWithAttributesEncodedMarker.length = (N * 103) - 1
        // Once we have N, we round up since the encoder adds partition ids until it runs _over_ the max length.
        //
        // After that, each marker should have the next N = (32000 - (emptyEncodedMarker.length))/103 partition IDs until the last page.
        long firstPageIdCount = (long)Math.ceil(((double)PartitionMetadata.MARKER_LENGTH_CUTOFF - (double)emptyWithAttributesEncodedMarker.length()) / 103.0);
        long middlePageIdCount = (long)Math.ceil(((double)PartitionMetadata.MARKER_LENGTH_CUTOFF - (double)emptyEncodedMarker.length()) / 103.0);

        List<String> output = new ArrayList<>();
        Iterator<String> iter = partitionIds.iterator();
        Set<String> ids = new TreeSet<>();
        for (int i = 0; i < firstPageIdCount && iter.hasNext(); i++) {
            ids.add(iter.next());
        }
        output.add(generateMarkerWithContent(ids, attributes));
        long partitionIdCount = partitionIds.size() - firstPageIdCount;

        while (partitionIdCount > 0) {
            ids = new TreeSet<>();
            for (int i = 0; i < middlePageIdCount && iter.hasNext(); i++) {
                ids.add(iter.next());
            }
            output.add(generateMarkerWithContent(ids, Collections.emptyMap()));
            partitionIdCount -= middlePageIdCount;
        }

        return output;
    }

    private String generateMarkerWithContent(Set<String> partitionIds, Map<String, Object> attributes) throws JsonProcessingException {
        String encodedAttributes = MAPPER.writeValueAsString(StepAttributes.serializeMapValues(attributes));
        return "{\"fluxMetadataVersion\":1,"
                + "\"partitionIds\":[\""
                + String.join("\",\"", partitionIds)
                + "\"],"
                + "\"encodedAdditionalAttributes\":" + encodedAttributes + "}";
    }
}
