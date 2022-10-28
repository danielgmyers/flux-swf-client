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

package com.danielgmyers.flux.signals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SignalTypeTest {
    @Test
    public void testGetSignalDataType() {
        Assertions.assertEquals(SignalType.FORCE_RESULT, new ForceResultSignalData().getSignalType());
        Assertions.assertEquals(SignalType.RETRY_NOW, new RetryNowSignalData().getSignalType());
        Assertions.assertEquals(SignalType.SCHEDULE_DELAYED_RETRY, new ScheduleDelayedRetrySignalData().getSignalType());
        Assertions.assertEquals(SignalType.DELAY_RETRY, new DelayRetrySignalData().getSignalType());
    }

    @Test
    public void testFromFriendlyName() {
        Assertions.assertEquals(SignalType.FORCE_RESULT, SignalType.fromFriendlyName(SignalType.FORCE_RESULT.getFriendlyName()));
        Assertions.assertEquals(SignalType.RETRY_NOW, SignalType.fromFriendlyName(SignalType.RETRY_NOW.getFriendlyName()));
        Assertions.assertEquals(SignalType.SCHEDULE_DELAYED_RETRY, SignalType.fromFriendlyName(SignalType.SCHEDULE_DELAYED_RETRY.getFriendlyName()));
        Assertions.assertEquals(SignalType.DELAY_RETRY, SignalType.fromFriendlyName(SignalType.DELAY_RETRY.getFriendlyName()));
        Assertions.assertEquals(null, SignalType.fromFriendlyName("FakeSignal"));
    }
}
