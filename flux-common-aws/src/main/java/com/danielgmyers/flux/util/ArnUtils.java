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

package com.danielgmyers.flux.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.arns.Arn;

/**
 * In AWS, ARNs look like one of these:
 *
 * <pre>
 * {@code
 *  arn:<partition>:<service>:<region>:<account>:<resourcetype>/resource
 *  arn:<partition>:<service>:<region>:<account>:<resourcetype>/resource/qualifier
 *  arn:<partition>:<service>:<region>:<account>:<resourcetype>/resource:qualifier
 *  arn:<partition>:<service>:<region>:<account>:<resourcetype>:resource
 *  arn:<partition>:<service>:<region>:<account>:<resourcetype>:resource:qualifier
 * }
 * </pre>
 *
 * The AWS SDK provides an Arn class for parsing these values.
 *
 * This class provides helpers for extracting specific parts of ARNs, without needing to duplicate validation logic everywhere.
 */
public final class ArnUtils {

    private static final Logger log = LoggerFactory.getLogger(ArnUtils.class);

    private ArnUtils() {}

    /**
     * Returns the qualifier portion of the provided ARN, if present.
     * If not present, or the input is invalid, returns null.
     */
    public static String extractQualifier(String arn) {
        try {
            Arn parsed = Arn.fromString(arn);
            return parsed.resource().qualifier().orElse(null);
        } catch (IllegalArgumentException e) {
            log.warn("The provided arn was invalid: {}", arn, e);
            return null;
        }
    }

}
