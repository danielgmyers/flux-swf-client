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

package com.danielgmyers.flux.clients.swf;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

/**
 * Allows users to provide SWF client configuration to be used to create a RemoteWorkflowExecutor.
 * Note that at a minimum, the AWS region and workflow domain must not be null.
 * If credentials is null, DefaultCredentialsProvider will be used.
 * If swfEndpoint and clientOverrideConfiguration are null they will be ignored.
 */
public class RemoteSwfClientConfig {
    private String awsRegion;
    private String swfEndpoint;
    private String workflowDomain;
    private AwsCredentialsProvider credentials;
    private ClientOverrideConfiguration clientOverrideConfiguration;
    private Boolean automaticallyTagExecutionsWithTaskList;

    public String getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    public String getSwfEndpoint() {
        return swfEndpoint;
    }

    public void setSwfEndpoint(String swfEndpoint) {
        this.swfEndpoint = swfEndpoint;
    }

    public String getWorkflowDomain() {
        return workflowDomain;
    }

    public void setWorkflowDomain(String workflowDomain) {
        this.workflowDomain = workflowDomain;
    }

    public AwsCredentialsProvider getCredentials() {
        return credentials;
    }

    public void setCredentials(AwsCredentialsProvider credentials) {
        this.credentials = credentials;
    }

    public ClientOverrideConfiguration getClientOverrideConfiguration() {
        return clientOverrideConfiguration;
    }

    public void setClientOverrideConfiguration(ClientOverrideConfiguration clientOverrideConfiguration) {
        this.clientOverrideConfiguration = clientOverrideConfiguration;
    }

    public Boolean getAutomaticallyTagExecutionsWithTaskList() {
        return automaticallyTagExecutionsWithTaskList;
    }

    public void setAutomaticallyTagExecutionsWithTaskList(Boolean automaticallyTagExecutionsWithTaskList) {
        this.automaticallyTagExecutionsWithTaskList = automaticallyTagExecutionsWithTaskList;
    }
}
