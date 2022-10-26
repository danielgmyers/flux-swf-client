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

package com.danielgmyers.flux.clients.swf.poller.signals;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.swf.model.WorkflowExecutionSignaledEventAttributes;

/**
 * Helper methods for dealing with signals.
 */
public final class SignalUtils {

    private SignalUtils() {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Decodes the data contained in a WorkflowExecutionSignaledEventAttributes object into a subclass of BaseSignalData.
     * If the data is invalid, or not recognized, or missing, returns null.
     */
    public static BaseSignalData decodeSignal(WorkflowExecutionSignaledEventAttributes signal) {
        SignalType signalType = SignalType.fromFriendlyName(signal.signalName());
        if (signalType == null) {
            return null;
        }
        try {
            BaseSignalData data = MAPPER.readValue(signal.input(), signalType.getSignalDataType());
            if (!data.isValidSignalInput()) {
                return null;
            }
            return data;
        } catch (IOException e) {
            // if we get this exception, the signal data was malformed, and we'll ignore the signal.
            return null;
        }
    }

    /**
     * Given a BaseSignalData object, returns an encoded string representing that signal data.
     */
    public static <T extends BaseSignalData> String encodeSignal(T signalData) throws JsonProcessingException {
        return MAPPER.writeValueAsString(signalData);
    }
}
