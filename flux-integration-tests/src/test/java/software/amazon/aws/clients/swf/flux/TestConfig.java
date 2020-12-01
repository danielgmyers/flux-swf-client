package software.amazon.aws.clients.swf.flux;

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
     * Retrieves the configured SWF endpoint.
     */
    public static String getSwfEndpoint() {
        return System.getProperty("swfEndpoint", null);
    }

    /**
     * Gets the region to be used in the Remote Workflow tests.
     */
    public static String getRemoteRegion() {
        return System.getProperty("remoteRegion", "us-east-1");
    }

    /**
     * Gets the SWF endpoint to be used in the Remote Workflow tests.
     */
    public static String getRemoteEndpoint() {
        return System.getProperty("remoteEndpoint", null);
    }

    /**
     * Generates Flux configuration that sets up very small worker pools to minimize the risk of multiple threads
     * throttling each other if different test suites are running concurrently.
     *
     * @param swfDomain - the swf domain to set in this configuration
     * @param workerPoolSize - the size of the thread pool for the deciders and workers.
     */
    public static FluxCapacitorConfig generateFluxConfig(String swfDomain, int workerPoolSize) {
        return generateFluxConfig(getAwsRegion(), getSwfEndpoint(), swfDomain, workerPoolSize);
    }

    /**
     * Generates Flux configuration that sets up very small worker pools to minimize the risk of multiple threads
     * throttling each other if different test suites are running concurrently.
     *
     * @param swfDomain - the swf domain to set in this configuration
     * @param workerPoolSize - the size of the thread pool for the deciders and workers.
     */
    public static FluxCapacitorConfig generateRemoteFluxConfig(String swfDomain, int workerPoolSize) {
        return generateFluxConfig(getRemoteRegion(), getRemoteEndpoint(), swfDomain, workerPoolSize);
    }

    private static FluxCapacitorConfig generateFluxConfig(String region, String endpoint, String domain, int poolSize) {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(region);
        if (endpoint != null) {
            config.setSwfEndpoint(endpoint); // endpoint is determined automatically from region if endpoint is not set
        }
        config.setSwfDomain(domain);

        TaskListConfig tasklistConfig = new TaskListConfig();
        tasklistConfig.setActivityTaskThreadCount(poolSize);
        tasklistConfig.setDecisionTaskThreadCount(poolSize);
        tasklistConfig.setPeriodicSubmitterThreadCount(1);

        config.putTaskListConfig(FluxCapacitorImpl.DEFAULT_TASK_LIST_NAME, tasklistConfig);
        return config;
    }
}
