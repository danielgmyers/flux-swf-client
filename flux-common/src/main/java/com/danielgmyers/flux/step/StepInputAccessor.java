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

package com.danielgmyers.flux.step;

/**
 * Provides an implementation-independent mechanism for retrieving step inputs from common code.
 */
public interface StepInputAccessor {
    /**
     * Returns the requested attribute as an object of the requested type.
     * Implementations _may_ convert the actual attribute value to the requested type if doing so makes sense;
     * otherwise, implementations should throw AttributeTypeMismatchException when the type is incorrect.
     *
     * If the attribute does not exist, this method should return null.
     */
    <T> T getAttribute(Class<T> requestedType, String attributeName);
}
