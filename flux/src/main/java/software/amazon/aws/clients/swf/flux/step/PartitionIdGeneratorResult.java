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

package software.amazon.aws.clients.swf.flux.step;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This can be used as the return value of a method annotated with @PartitionIdGenerator.
 *
 * When this return type is used (rather than List&lt;String&gt;), the following behavior is observed:
 * - The partition ids in the partitionIds set are used to generate new partitions.
 *
 * - Any attributes returned in the additionalAttributes map will be added to the attributes passed to all future
 *   steps in the workflow (including the execuctions of this same partitioned step), regardless of what the result
 *   code is. These attribute values are subject to the same validation requirements as standard StepResult attributes.
 */
public final class PartitionIdGeneratorResult {

    private Set<String> partitionIds;
    private Map<String, Object> additionalAttributes;

    private PartitionIdGeneratorResult(Set<String> partitionIds) {
        this.partitionIds = partitionIds;
        this.additionalAttributes = new HashMap<>();
    }

    public static PartitionIdGeneratorResult create() {
        return new PartitionIdGeneratorResult(new HashSet<>());
    }

    public static PartitionIdGeneratorResult create(Set<String> partitionIds) {
        return new PartitionIdGeneratorResult(new HashSet<>(partitionIds));
    }

    public PartitionIdGeneratorResult withPartitionIds(Set<String> partitionIds) {
        this.partitionIds.addAll(partitionIds);
        return this;
    }

    public PartitionIdGeneratorResult withAttribute(String name, Object value) {
        additionalAttributes.put(name, value);
        return this;
    }

    public PartitionIdGeneratorResult withAttributes(Map<String, Object> attributes) {
        additionalAttributes.putAll(attributes);
        return this;
    }

    public Set<String> getPartitionIds() {
        return partitionIds;
    }

    public Map<String, Object> getAdditionalAttributes() {
        return additionalAttributes;
    }
}
