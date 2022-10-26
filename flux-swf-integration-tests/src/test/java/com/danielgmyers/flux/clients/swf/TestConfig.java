package com.danielgmyers.flux.clients.swf;

import com.danielgmyers.flux.wf.Workflow;

/**
 * Utility class to store/access configuration data provided to the tests.
 *
 * Unless otherwise specified:
 *  - Assumes us-west-2 is "local".
 *  - Assumes us-east-1 is "remote".
 */
public final class TestConfig {

    private TestConfig() {}

    /**
     * Retrieves the configured AWS region.
     */
    public static String getAwsRegion() {
        return System.getProperty("awsRegion", "us-west-2");
    }

    /**
     * Retrieves the configured remote region.
     */
    public static RemoteSwfClientConfig getRemoteClientConfig() {
        RemoteSwfClientConfig config = new RemoteSwfClientConfig();
        config.setAwsRegion(System.getProperty("remoteRegion", "us-east-1"));
        config.setSwfEndpoint(System.getProperty("remoteEndpoint", null));
        return config;
    }

    /**
     * Generates Flux configuration that sets up very small worker pools to minimize the risk of multiple threads
     * throttling each other if different test suites are running concurrently.
     *
     * @param swfDomain - the swf domain to set in this configuration
     * @param workerPoolSize - the size of the thread pool for the deciders and workers.
     */
    public static FluxCapacitorConfig generateFluxConfig(String swfDomain, int workerPoolSize) {
        return generateFluxConfig(getAwsRegion(), swfDomain, workerPoolSize);
    }

    /**
     * Generates Flux configuration that sets up very small worker pools to minimize the risk of multiple threads
     * throttling each other if different test suites are running concurrently.
     *
     * @param swfDomain - the swf domain to set in this configuration
     * @param workerPoolSize - the size of the thread pool for the deciders and workers.
     */
    public static FluxCapacitorConfig generateRemoteFluxConfig(String swfDomain, int workerPoolSize) {
        RemoteSwfClientConfig remoteConfig = getRemoteClientConfig();
        return generateFluxConfig(remoteConfig.getAwsRegion(), swfDomain, workerPoolSize);
    }

    private static FluxCapacitorConfig generateFluxConfig(String region, String domain, int poolSize) {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(region);
        config.setSwfDomain(domain);

        TaskListConfig tasklistConfig = new TaskListConfig();
        tasklistConfig.setActivityTaskThreadCount(poolSize);
        tasklistConfig.setDecisionTaskThreadCount(poolSize);
        tasklistConfig.setPeriodicSubmitterThreadCount(1);

        config.putTaskListConfig(Workflow.DEFAULT_TASK_LIST_NAME, tasklistConfig);
        return config;
    }
}
