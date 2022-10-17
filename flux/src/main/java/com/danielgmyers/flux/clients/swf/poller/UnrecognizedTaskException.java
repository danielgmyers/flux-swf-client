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

package com.danielgmyers.flux.clients.swf.poller;

/**
 * Indicates that a task was scheduled that the activity worker does not know about.
 * Typically this will happen during deployment after adding new steps;
 * if it happens after removing a step (or rolling back a deployment)
 * the affected workflow will be stuck until terminated or the activity
 * is made available again.
 */
public class UnrecognizedTaskException extends RuntimeException {

    public UnrecognizedTaskException(String message) {
        super(message);
    }

}
