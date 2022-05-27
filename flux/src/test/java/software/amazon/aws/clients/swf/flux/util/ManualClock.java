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

package software.amazon.aws.clients.swf.flux.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class ManualClock extends Clock {

    private Instant curTime;

    public ManualClock() {
        this.curTime = Instant.now();
    }

    public ManualClock(Instant startTime) {
        this.curTime = startTime;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return curTime;
    }

    @Override
    public long millis() {
        return curTime.toEpochMilli();
    }

    public Instant forward(Duration amount) {
        curTime = curTime.plus(amount);
        return curTime;
    }

    public Instant rewind(Duration amount) {
        curTime = curTime.minus(amount);
        return curTime;
    }
}
