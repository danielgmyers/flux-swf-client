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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.danielgmyers.flux.clients.swf.IdUtils;
import com.danielgmyers.flux.clients.swf.step.PartitionIdGeneratorResult;
import com.danielgmyers.flux.clients.swf.step.StepAttributes;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.Test;


public class PartitionMetadataTest {

    @Test
    public void testFromPartitionIdGeneratorResult_NoAttributes() {
        Set<String> partitionIds = new TreeSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);

        Assert.assertEquals(partitionIds, metadata.getPartitionIds());

        Assert.assertEquals(Collections.emptyMap(), metadata.getEncodedAdditionalAttributes());
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

        Assert.assertEquals(partitionIds, metadata.getPartitionIds());

        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);
        Assert.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());
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

        String expectedJson = "{\"partitionIds\":[\"p1\",\"p2\",\"p3\"],"
                              + "\"encodedAdditionalAttributes\":"
                              +   "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}"
                              + "}";

        List<String> detailsList = metadata.toMarkerDetailsList();
        Assert.assertEquals(1, detailsList.size());
        Assert.assertEquals(expectedJson, detailsList.get(0));
    }

    @Test
    public void testToMarkerDetailsList_MultipleMarkers() throws JsonProcessingException {
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        PartitionIdGeneratorResult genResult = PartitionIdGeneratorResult.create(partitionIds).withAttributes(attributes);
        PartitionMetadata metadata = PartitionMetadata.fromPartitionIdGeneratorResult(genResult);

        // The expectedFirstJson string is 109 characters long excluding the partition ID array.
        // Each partition ID has double-quotes around it and the IDs are comma-separated.
        // So a list of N 100-character partition IDs will use up N*100 + N*2 + (N-1) characters, or (N*103)-1.
        // To determine how many partition IDs are on the first page, we need to solve this equation:
        // 32000 - 109 = (N * 103) - 1
        // which is 31892 = (N*103)
        // so N = 31892 / 103 = 309.6 and since we add partition IDs until we run _over the max length, we round up.
        // The first marker should have 310 partition IDs.
        //
        // After that, each marker should have the next N = 32001/103 = 310.6  or 311 partition IDs until the last page.
        // The last page will have 1000 - 310 - 311 - 311 = 68 partition IDs.

        Iterator<String> iter = partitionIds.iterator();
        Set<String> page1 = new TreeSet<>();
        for (int i = 0; i < 310; i++) {
            page1.add(iter.next());
        }
        Set<String> page2 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page2.add(iter.next());
        }
        Set<String> page3 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page3.add(iter.next());
        }
        Set<String> page4 = new TreeSet<>();
        while (iter.hasNext()) {
            page4.add(iter.next());
        }
        Assert.assertEquals(68, page4.size());

        String expectedJson1 = "{\"partitionIds\":[\""
                               + String.join("\",\"", page1)
                               + "\"],"
                               + "\"encodedAdditionalAttributes\":"
                               + "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}}";
        Assert.assertTrue(32768 > expectedJson1.length());

        String expectedJson2 = "{\"partitionIds\":[\""
                               + String.join("\",\"", page2)
                               + "\"],\"encodedAdditionalAttributes\":{}}";
        Assert.assertTrue(32768 > expectedJson2.length());

        String expectedJson3 = "{\"partitionIds\":[\""
                               + String.join("\",\"", page3)
                               + "\"],\"encodedAdditionalAttributes\":{}}";
        Assert.assertTrue(32768 > expectedJson3.length());

        String expectedJson4 = "{\"partitionIds\":[\""
                               + String.join("\",\"", page4)
                               + "\"],\"encodedAdditionalAttributes\":{}}";
        Assert.assertTrue(32768 > expectedJson4.length());

        List<String> detailsList = metadata.toMarkerDetailsList();
        Assert.assertEquals(4, detailsList.size());
        Assert.assertEquals(expectedJson1, detailsList.get(0));
        Assert.assertEquals(expectedJson2, detailsList.get(1));
        Assert.assertEquals(expectedJson3, detailsList.get(2));
        Assert.assertEquals(expectedJson4, detailsList.get(3));
    }

    @Test
    public void testFromMarkerDetailsList_OneMarker() throws JsonProcessingException {
        String inputJson = "{\"partitionIds\":[\"p1\",\"p2\",\"p3\"],"
                              + "\"encodedAdditionalAttributes\":"
                              +   "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}"
                              + "}";
        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(Collections.singletonList(inputJson));

        Set<String> partitionIds = new HashSet<>();
        partitionIds.add("p1");
        partitionIds.add("p2");
        partitionIds.add("p3");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);

        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);

        Assert.assertEquals(partitionIds, metadata.getPartitionIds());
        Assert.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());
    }

    @Test
    public void testFromMarkerDetailsList_InvalidMarker() throws JsonProcessingException {
        String inputJson = "this is not valid json {";
        Assert.assertNull(PartitionMetadata.fromMarkerDetailsList(Collections.singletonList(inputJson)));
    }

    @Test
    public void testFromMarkerDetailsList_MultipleMarkers() throws JsonProcessingException {
        // We'll use a TreeSet so that the set is consistently sorted when we iterate through it.
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        // The expectedFirstJson string is 109 characters long excluding the partition ID array.
        // Each partition ID has double-quotes around it and the IDs are comma-separated.
        // So a list of N 100-character partition IDs will use up N*100 + N*2 + (N-1) characters, or (N*103)-1.
        // To determine how many partition IDs are on the first page, we need to solve this equation:
        // 32000 - 109 = (N * 103) - 1
        // which is 31892 = (N*103)
        // so N = 31892 / 103 = 309.6 and since we add partition IDs until we run _over the max length, we round up.
        // The first marker should have 310 partition IDs.
        //
        // After that, each marker should have the next N = 32001/103 = 310.6  or 311 partition IDs until the last page.
        // The last page will have 1000 - 310 - 311 - 311 = 68 partition IDs.

        Iterator<String> iter = partitionIds.iterator();
        Set<String> page1 = new TreeSet<>();
        for (int i = 0; i < 310; i++) {
            page1.add(iter.next());
        }
        Set<String> page2 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page2.add(iter.next());
        }
        Set<String> page3 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page3.add(iter.next());
        }
        Set<String> page4 = new TreeSet<>();
        while (iter.hasNext()) {
            page4.add(iter.next());
        }
        Assert.assertEquals(68, page4.size());

        String inputJson1 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page1)
                            + "\"],"
                            + "\"encodedAdditionalAttributes\":"
                            + "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}}";

        String inputJson2 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page2)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        String inputJson3 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page3)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        String inputJson4 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page4)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(Arrays.asList(inputJson1, inputJson2, inputJson3, inputJson4));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);
        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);

        Assert.assertEquals(partitionIds, metadata.getPartitionIds());
        Assert.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());
    }

    @Test
    public void testFromMarkerDetailsList_IgnoresInvalidExtraMarker() throws JsonProcessingException {
        // We'll use a TreeSet so that the set is consistently sorted when we iterate through it.
        Set<String> partitionIds = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            partitionIds.add(IdUtils.randomId(100));
        }

        // The expectedFirstJson string is 109 characters long excluding the partition ID array.
        // Each partition ID has double-quotes around it and the IDs are comma-separated.
        // So a list of N 100-character partition IDs will use up N*100 + N*2 + (N-1) characters, or (N*103)-1.
        // To determine how many partition IDs are on the first page, we need to solve this equation:
        // 32000 - 109 = (N * 103) - 1
        // which is 31892 = (N*103)
        // so N = 31892 / 103 = 309.6 and since we add partition IDs until we run _over the max length, we round up.
        // The first marker should have 310 partition IDs.
        //
        // After that, each marker should have the next N = 32001/103 = 310.6  or 311 partition IDs until the last page.
        // The last page will have 1000 - 310 - 311 - 311 = 68 partition IDs.

        Iterator<String> iter = partitionIds.iterator();
        Set<String> page1 = new TreeSet<>();
        for (int i = 0; i < 310; i++) {
            page1.add(iter.next());
        }
        Set<String> page2 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page2.add(iter.next());
        }
        Set<String> page3 = new TreeSet<>();
        for (int i = 0; i < 311; i++) {
            page3.add(iter.next());
        }
        Set<String> page4 = new TreeSet<>();
        while (iter.hasNext()) {
            page4.add(iter.next());
        }
        Assert.assertEquals(68, page4.size());

        String inputJson1 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page1)
                            + "\"],"
                            + "\"encodedAdditionalAttributes\":"
                            + "{\"number\":\"42\",\"string\":\"\\\"some words here\\\"\",\"yes\":\"true\"}}";

        String inputJson2 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page2)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        String inputJson3 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page3)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        String inputJson4 = "{\"partitionIds\":[\""
                            + String.join("\",\"", page4)
                            + "\"],\"encodedAdditionalAttributes\":{}}";

        String invalidJson = "this is not valid json {";

        PartitionMetadata metadata = PartitionMetadata.fromMarkerDetailsList(Arrays.asList(inputJson1, inputJson2, invalidJson, inputJson3, inputJson4));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("number", 42L);
        attributes.put("string", "some words here");
        attributes.put("yes", true);
        Map<String, String> encodedAttributes = StepAttributes.serializeMapValues(attributes);

        Assert.assertEquals(partitionIds, metadata.getPartitionIds());
        Assert.assertEquals(encodedAttributes, metadata.getEncodedAdditionalAttributes());
    }
}
