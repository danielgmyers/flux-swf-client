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

package software.amazon.aws.clients.swf.flux.wf.graph;

/**
 * Indicates that there was a problem building a workflow graph,
 * either in the setup methods (e.g. addStep) or in the build method.
 */
public class WorkflowGraphBuildException extends RuntimeException {

    /**
     * Creates a WorkflowGraphBuildException.
     * @param message The message to include in the exception.
     */
    public WorkflowGraphBuildException(String message) {
        super(message);
    }

    /**
     * Creates a WorkflowGraphBuildException.
     * @param message The message to include in the exception.
     * @param cause   The cause of the exception.
     */
    public WorkflowGraphBuildException(String message, Throwable cause) {
        super(message, cause);
    }

}
