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

package software.amazon.aws.clients.swf.flux.step;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

public class StepAttributesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testNullEncoding() {
        Assert.assertNull(StepAttributes.encode(null));
        for (Class<?> t : StepAttributes.ALLOWED_TYPES) {
            Assert.assertNull(StepAttributes.decode(t, null));
        }
        Assert.assertEquals(Collections.emptyMap(), StepAttributes.decode(Map.class, null));
    }

    @Test
    public void testBlankStringEncoding() {
        for (Class<?> t : StepAttributes.ALLOWED_TYPES) {
            if (t == String.class) {
                // blank non-null strings get encoded as the string ""
                Assert.assertEquals("\"\"", StepAttributes.encode(""));
            } else {
                Assert.assertNull(StepAttributes.decode(t, ""));
            }
        }
        Assert.assertEquals(Collections.emptyMap(), StepAttributes.decode(Map.class, ""));
    }

    @Test
    public void testBasicStringMapEncoding() throws JsonProcessingException {
        Map<String, String> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", "zap");

        String encoded = StepAttributes.encode(data);
        Assert.assertEquals(MAPPER.writeValueAsString(data), encoded);

        Map<String, String> decoded = StepAttributes.decode(Map.class, encoded);
        Assert.assertEquals(data, decoded);
    }

    @Test
    public void testSerializeMapValues() throws JsonProcessingException {
        Map<String, Object> data = new HashMap<>();
        data.put("long", 7L);
        data.put("boolean", true);
        data.put("date", Instant.now());
        data.put("string", "foobar");
        data.put("map", Collections.singletonMap("key", "value"));

        Map<String, String> encoded = StepAttributes.serializeMapValues(data);
        Assert.assertEquals(data.keySet(), encoded.keySet());

        for (Map.Entry<String, String> e : encoded.entrySet()) {
            Assert.assertEquals(StepAttributes.encode(data.get(e.getKey())), e.getValue());
        }
    }

    @Test
    public void testBooleanEncoding() {
        String encoded = StepAttributes.encode(true);
        Assert.assertTrue(StepAttributes.decode(Boolean.class, encoded));

        encoded = StepAttributes.encode(false);
        Assert.assertFalse(StepAttributes.decode(Boolean.class, encoded));
    }

    @Test
    public void testLongEncoding() {
        Long data = 7L;
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(Long.class, encoded));
    }

    @Test
    public void testStringEncoding() {
        String data = "hello world!";
        String encoded = StepAttributes.encode(data);

        Assert.assertEquals(data, StepAttributes.decode(String.class, encoded));
    }

    @Test
    public void testDateAndInstantEncoding() {
        Instant now = Instant.now();
        Date date = Date.from(now);

        String date_encoded = StepAttributes.encode(date);
        String instant_encoded = StepAttributes.encode(now);

        // we want these to both be encoded the same way
        Assert.assertEquals(date_encoded, instant_encoded);

        // we want the encoded form to be decodeable as either class
        Assert.assertEquals(date, StepAttributes.decode(Date.class, date_encoded));

        // the serialized form is truncated to millis, so we need to truncate our local object for comparison.
        Assert.assertEquals(now.truncatedTo(ChronoUnit.MILLIS), StepAttributes.decode(Instant.class, date_encoded));
    }

    @Test
    public void testIsValidAttributeClass() {
        Assert.assertTrue(StepAttributes.isValidAttributeClass(Boolean.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(Long.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(Date.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(String.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(Instant.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(Map.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(HashMap.class));
        Assert.assertTrue(StepAttributes.isValidAttributeClass(TreeMap.class));

        Assert.assertFalse(StepAttributes.isValidAttributeClass(Integer.class));
        Assert.assertFalse(StepAttributes.isValidAttributeClass(Duration.class));
        Assert.assertFalse(StepAttributes.isValidAttributeClass(Set.class));
        Assert.assertFalse(StepAttributes.isValidAttributeClass(List.class));
    }
}
