package com.danielgmyers.flux.clients.swf.tests;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.clients.swf.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.swf.RemoteWorkflowExecutor;
import com.danielgmyers.flux.clients.swf.TestConfig;
import com.danielgmyers.flux.clients.swf.metrics.NoopMetricRecorderFactory;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.SwfClientBuilder;
import software.amazon.awssdk.services.swf.model.ExecutionTimeFilter;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryRequest;
import software.amazon.awssdk.services.swf.model.GetWorkflowExecutionHistoryResponse;
import software.amazon.awssdk.services.swf.model.ListClosedWorkflowExecutionsRequest;
import software.amazon.awssdk.services.swf.model.ListClosedWorkflowExecutionsResponse;
import software.amazon.awssdk.services.swf.model.ListOpenWorkflowExecutionsRequest;
import software.amazon.awssdk.services.swf.model.SignalWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.TerminateWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;
import software.amazon.awssdk.services.swf.model.WorkflowExecution;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionFilter;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;
import software.amazon.awssdk.services.swf.paginators.ListOpenWorkflowExecutionsIterable;

/**
 * Shared initialization for the tests.
 */
@Disabled
public abstract class WorkflowTestBase {

    private SwfClient swfClient;
    private FluxCapacitor capacitor;

    protected void executeWorkflow(Class<? extends Workflow> workflowClass, String workflowId, Map<String, Object> input) {
        capacitor.executeWorkflow(workflowClass, workflowId, input);
    }

    protected RemoteWorkflowExecutor getRemoteWorkflowExecutor() {
        return capacitor.getRemoteWorkflowExecutor(TestConfig.getRemoteRegion(), TestConfig.getRemoteEndpoint(),
                                                   DefaultCredentialsProvider.create(), getWorkflowDomain());
    }

    abstract List<Workflow> getWorkflowsForTest();

    /**
     * Test classes should override this so that the logger has the test class name in it instead of WorkflowTestBase.
     */
    abstract Logger getLogger();

    int getWorkerPoolThreadCount() {
        return 1;
    }

    /**
     * Configures an SWF client and the FluxCapacitor.
     */
    @BeforeAll
    public void setUpFluxCapacitor() {
        swfClient = createSwfClient(true);

        terminateOpenWorkflowExecutions(swfClient);

        capacitor = createFluxCapacitor(true, getWorkflowsForTest());
    }

    protected SwfClient createSwfClient(boolean localRegion) {
        SwfClientBuilder swfClientBuilder = SwfClient.builder().credentialsProvider(DefaultCredentialsProvider.create());
        if (localRegion) {
            if (TestConfig.getSwfEndpoint() != null) {
                swfClientBuilder.endpointOverride(URI.create(TestConfig.getSwfEndpoint()));
            }
            swfClientBuilder.region(Region.of(TestConfig.getAwsRegion()));
        } else {
            if (TestConfig.getRemoteEndpoint() != null) {
                swfClientBuilder.endpointOverride(URI.create(TestConfig.getRemoteEndpoint()));
            }
            swfClientBuilder.region(Region.of(TestConfig.getRemoteRegion()));
        }
        return swfClientBuilder.build();
    }

    protected FluxCapacitor createFluxCapacitor(boolean localRegion, List<Workflow> workflows) {
        getLogger().info(String.format("Initializing %s Flux with domain %s...", (localRegion ? "local" : "remote"), getWorkflowDomain()));
        FluxCapacitorConfig config;
        if (localRegion) {
            config = TestConfig.generateFluxConfig(getWorkflowDomain(), getWorkerPoolThreadCount());
        }  else {
            config = TestConfig.generateRemoteFluxConfig(getWorkflowDomain(), getWorkerPoolThreadCount());
        }
        updateFluxCapacitorConfig(config);
        FluxCapacitor capacitor = FluxCapacitorFactory.create(new NoopMetricRecorderFactory(),
                                                              DefaultCredentialsProvider.create(), config);
        capacitor.initialize(workflows);
        getLogger().info(String.format("Finished initializing %s Flux with domain %s...",
                               (localRegion ? "local" : "remote"), getWorkflowDomain()));
        return capacitor;
    }

    protected void updateFluxCapacitorConfig(FluxCapacitorConfig config) {
        // do nothing by default;
    }

    /**
     * Cleans up Flux.
     */
    @AfterAll
    public void cleanUpFluxCapacitor() throws InterruptedException {
        getLogger().info("Shutting down Flux with domain " + getWorkflowDomain() + "...");
        capacitor.shutdown();

        terminateOpenWorkflowExecutions(swfClient);
    }

    protected void terminateOpenWorkflowExecutions(SwfClient swf) {
        getLogger().info("Looking for workflow executions to terminate...");

        Instant now = Instant.now();
        ListOpenWorkflowExecutionsRequest request = ListOpenWorkflowExecutionsRequest.builder()
                .domain(getWorkflowDomain())
                .startTimeFilter(ExecutionTimeFilter.builder().oldestDate(now.minus(Duration.ofDays(21))).build())
                .build();

        ListOpenWorkflowExecutionsIterable infos = swf.listOpenWorkflowExecutionsPaginator(request);

        boolean found = false;
        for (WorkflowExecutionInfo info: infos.executionInfos()) {
            found = true;
            getLogger().info("Found execution of " + info.workflowType().name() + " with id " + info.execution().workflowId()
                    + ", terminating...");
            try {
                swf.terminateWorkflowExecution(TerminateWorkflowExecutionRequest.builder().domain(getWorkflowDomain())
                                                       .workflowId(info.execution().workflowId()).build());
            } catch (UnknownResourceException e) {
                getLogger().info("Execution " + info.execution().workflowId()
                                 + " terminated on its own before we could terminate it.", e);
            }
        }

        if (!found) {
            getLogger().info("No workflow executions eligible for termination found.");
        }
    }

    void signalWorkflowExecution(String workflowId, String signalName, String signalContent) {
        getLogger().info(String.format("Sending %s signal to workflow %s with content: %n%s",
                               signalName, workflowId, signalContent));
        SignalWorkflowExecutionRequest request = SignalWorkflowExecutionRequest.builder()
                .domain(getWorkflowDomain()).workflowId(workflowId).signalName(signalName).input(signalContent).build();
        swfClient.signalWorkflowExecution(request);
    }

    GetWorkflowExecutionHistoryResponse getWorkflowExecutionHistory(String workflowId, String runId) {
        GetWorkflowExecutionHistoryRequest request = GetWorkflowExecutionHistoryRequest.builder()
                .domain(getWorkflowDomain())
                .execution(WorkflowExecution.builder().workflowId(workflowId).runId(runId).build())
                .build();
        return swfClient.getWorkflowExecutionHistory(request);
    }

    /**
     * Waits for the specified workflowId to show up in the list of closed workflow executions.
     */
    WorkflowExecutionInfo waitForWorkflowCompletion(String workflowId, Duration timeout) throws InterruptedException {
        Instant now = Instant.now();
        Instant stopWaiting = now.plus(timeout);

        getLogger().info("Waiting for workflow " + workflowId + " to close for up to " + timeout.getSeconds() + " seconds.");

        while (true) {
            try {
                ListClosedWorkflowExecutionsRequest request = ListClosedWorkflowExecutionsRequest.builder()
                        .domain(getWorkflowDomain())
                        .executionFilter(WorkflowExecutionFilter.builder().workflowId(workflowId).build())
                        .startTimeFilter(ExecutionTimeFilter.builder().oldestDate(now.minus(Duration.ofMinutes(15))).build())
                        .build();

                ListClosedWorkflowExecutionsResponse infos = swfClient.listClosedWorkflowExecutions(request);
                if (infos.executionInfos() != null && !infos.executionInfos().isEmpty()) {
                    getLogger().info("Found closed execution for workflow id " + workflowId
                                     + ", runId: " + infos.executionInfos().get(0).execution().runId());
                    return infos.executionInfos().get(0);
                }
            } catch (UnknownResourceException e) {
                getLogger().warn("Got UnknownResourceException trying to list closed workflows.", e);
            }
            getLogger().info("Workflow with id " + workflowId + " does not appear to be closed yet.");

            if (now.isAfter(stopWaiting)) {
                String message = "Timed out waiting for completion of workflow with id: " + workflowId;
                getLogger().warn(message);
                throw new RuntimeException(message);
            }

            now = Instant.now();
            TimeUnit.SECONDS.sleep(2);
        }
    }

    /**
     * Returns the SWF domain that the child classes should use when configuring Flux.
     * Each test suite should use its own domain to minimize interference with each other when run in parallel;
     * task polling is per domain, so this ensures test suites will only poll for their own workflows.
     */
    protected String getWorkflowDomain() {
        // This will return the canonical name of the class that extends WorkflowTestBase,
        // e.g. com.danielgmyers.flux.clients.swf.tests.BasicWorkflowTest
        // Periods are valid characters in SWF domains so this shouldn't be a problem.
        return this.getClass().getCanonicalName();
    }
}
