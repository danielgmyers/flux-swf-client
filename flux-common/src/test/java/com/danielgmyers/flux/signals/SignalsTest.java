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

public class SignalsTest {

    @Test
    public void testBaseSignalData_isValidSignalInput() {
        BaseSignalData signal = new BaseSignalData() {
            @Override
            public SignalType getSignalType() {
                // for this test, this doesn't matter.
                return null;
            }
        };

        // activityId is null, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setActivityId("");
        // activityId is blank, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setActivityId("SomeActivityId");
        // activityId is not null nor empty, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());
    }

    @Test
    public void testDelayRetrySignalData_isValidSignalInput() {
        DelayRetrySignalData signal = new DelayRetrySignalData();
        signal.setActivityId("SomeActivityId");

        // delayInSeconds is null, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setDelayInSeconds(-7);
        // delayInSeconds is negative, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setDelayInSeconds(0);
        // delayInSeconds is zero, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());

        signal.setDelayInSeconds(100);
        // delayInSeconds is positive, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());

        // verify that we're still requiring activityId
        signal.setActivityId(null);
        Assertions.assertFalse(signal.isValidSignalInput());
    }

    @Test
    public void testScheduleDelayedRetrySignalData_isValidSignalInput() {
        ScheduleDelayedRetrySignalData signal = new ScheduleDelayedRetrySignalData();
        signal.setActivityId("SomeActivityId");

        // delayInSeconds is null, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setDelayInSeconds(-7);
        // delayInSeconds is negative, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setDelayInSeconds(0);
        // delayInSeconds is zero, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());

        signal.setDelayInSeconds(100);
        // delayInSeconds is positive, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());

        // verify that we're still requiring activityId
        signal.setActivityId(null);
        Assertions.assertFalse(signal.isValidSignalInput());
    }

    @Test
    public void testForceResultSignalData_isValidSignalInput() {
        ForceResultSignalData signal = new ForceResultSignalData();
        signal.setActivityId("SomeActivityId");

        // resultCode is null, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setResultCode("");
        // resultCode is blank, so the signal is invalid
        Assertions.assertFalse(signal.isValidSignalInput());

        signal.setResultCode("MyResultCode");
        // resultCode is not null nor blank, so the signal is valid
        Assertions.assertTrue(signal.isValidSignalInput());

        // verify that we're still requiring activityId
        signal.setActivityId(null);
        Assertions.assertFalse(signal.isValidSignalInput());
    }

    @Test
    public void testRetryNowSignalData_isValidSignalInput() {
        RetryNowSignalData signal = new RetryNowSignalData();
        signal.setActivityId("SomeActivityId");

        // retryNow doesn't have any other data
        Assertions.assertTrue(signal.isValidSignalInput());

        // verify that we're still requiring activityId
        signal.setActivityId(null);
        Assertions.assertFalse(signal.isValidSignalInput());
    }


}
