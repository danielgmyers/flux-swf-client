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

package software.amazon.aws.clients.swf.flux.poller;

import software.amazon.aws.clients.swf.flux.step.WorkflowStep;

/**
 * Utility class for the DecisionTaskPoller to choose what step to execute next (if any).
 *
 * Package-private for access in tests.
 */
final class NextStepSelection {
    private static final NextStepSelection CLOSE_WORKFLOW = new NextStepSelection(null, true);
    private static final NextStepSelection UNKNOWN = new NextStepSelection(null, false);

    private WorkflowStep nextStep;
    private boolean closeWorkflow;

    public static NextStepSelection closeWorkflow() {
        return CLOSE_WORKFLOW;
    }

    public static NextStepSelection unknownResultCode() {
        return UNKNOWN;
    }

    public static NextStepSelection scheduleNextStep(WorkflowStep step) {
        return new NextStepSelection(step, false);
    }

    private NextStepSelection(WorkflowStep nextStep, boolean closeWorkflow) {
        this.nextStep = nextStep;
        this.closeWorkflow = closeWorkflow;
    }

    public boolean isNextStepUnknown() {
        return nextStep == null && !closeWorkflow;
    }

    public boolean workflowShouldClose() {
        return closeWorkflow;
    }

    public WorkflowStep getNextStep() {
        return nextStep;
    }
}
