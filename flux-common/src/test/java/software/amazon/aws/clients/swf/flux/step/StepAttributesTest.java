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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

public class StepAttributesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testEncodeReturnsNullWithNullInput() {
        Assert.assertNull(StepAttributes.encode(null));
    }

    @Test
    public void testEncodeProducesJson() throws JsonProcessingException {
        Map<String, String> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", "zap");

        String encoded = StepAttributes.encode(data);
        Assert.assertEquals(MAPPER.writeValueAsString(data), encoded);
    }

    @Test
    public void testDecodeReturnsEmptyMapWithNullInput() {
        Assert.assertEquals(Collections.emptyMap(), StepAttributes.decode(Map.class, null));
    }

    @Test
    public void testDecodeReturnsEmptyMapWithBlankInput() {
        Assert.assertEquals(Collections.emptyMap(), StepAttributes.decode(Map.class, ""));
    }

    @Test
    public void testDecodeProducesCorrectMap() {
        Map<String, String> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", "zap");
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(Map.class, encoded));
    }

    @Test
    public void testDecodeProducesCorrectBoolean() {
        Boolean data = true;
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(Boolean.class, encoded));
    }

    @Test
    public void testDecodeProducesCorrectLong() {
        Long data = 7L;
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(Long.class, encoded));
    }

    @Test
    public void testDecodeProducesCorrectString() {
        String data = "hello world!";
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(String.class, encoded));
    }
}
