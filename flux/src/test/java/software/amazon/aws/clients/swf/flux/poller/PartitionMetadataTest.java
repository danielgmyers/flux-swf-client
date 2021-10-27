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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import software.amazon.aws.clients.swf.flux.step.PartitionIdGeneratorResult;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;

public class PartitionMetadataTest {

    @Test
    public void testFromPartitionIdGeneratorResult_NoAttributes() {
        Set<String> partitionIds = new HashSet<>();
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
        Set<String> partitionIds = new HashSet<>();
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
}
