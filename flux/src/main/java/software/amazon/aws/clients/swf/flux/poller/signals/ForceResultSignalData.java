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

package software.amazon.aws.clients.swf.flux.poller.signals;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Stores the data related to a ForceResult signal.
 */
public class ForceResultSignalData extends BaseSignalData {

    private String resultCode;

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    @Override
    @JsonIgnore
    public SignalType getSignalType() {
        return SignalType.FORCE_RESULT;
    }

    @Override
    public boolean isValidSignalInput() {
        return super.isValidSignalInput() && (resultCode != null && !resultCode.isEmpty());
    }
}
