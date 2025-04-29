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

package com.danielgmyers.flux.clients.sfn;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.RemoteWorkflowExecutor;
import com.danielgmyers.flux.WorkflowStatusChecker;
import com.danielgmyers.flux.clients.sfn.poller.ActivityTaskPoller;
import com.danielgmyers.flux.clients.sfn.util.SfnArnFormatter;
import com.danielgmyers.flux.ex.FluxException;
import com.danielgmyers.flux.poller.TaskNaming;
import com.danielgmyers.flux.step.StepAttributes;
import com.danielgmyers.flux.step.WorkflowStep;
import com.danielgmyers.flux.threads.BlockOnSubmissionThreadPoolExecutor;
import com.danielgmyers.flux.threads.ThreadUtils;
import com.danielgmyers.flux.util.AwsRetryUtils;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.flux.wf.graph.WorkflowGraphNode;
import com.danielgmyers.metrics.MetricRecorder;
import com.danielgmyers.metrics.MetricRecorderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.SfnClientBuilder;
import software.amazon.awssdk.services.sfn.model.ActivityListItem;
import software.amazon.awssdk.services.sfn.model.CreateActivityRequest;
import software.amazon.awssdk.services.sfn.model.ListActivitiesRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;

/**
 * The primary class through which the Flux library is used at runtime.
 */
public class FluxCapacitorImpl implements FluxCapacitor {

    private static final Logger log = LoggerFactory.getLogger(FluxCapacitorImpl.class);

    private static final String LIST_ACTIVITIES_METRIC_PREFIX = "Flux.ListActivities";
    private static final String CREATE_ACTIVITY_METRIC_PREFIX = "Flux.CreateActivity";

    private final SfnClient sfn;
    private final FluxCapacitorConfig config;
    private final MetricRecorderFactory metricsFactory;
    private final Clock clock;

    private final Map<Class<? extends Workflow>, Workflow> workflowsByClass;
    private final Map<String, WorkflowStep> activitiesByName;
    private final Map<String, Workflow> workflowsByActivityName;

    private Map<String, ScheduledExecutorService> activityTaskPollerThreadsPerActivity;
    private Map<String, BlockOnSubmissionThreadPoolExecutor> workerThreadsPerTaskList;
    private ScheduledExecutorService periodicWorkflowScheduler;

    // The default throttling refill rate for the CreateActivity APIs is 1 per second,
    // so there's no sense retrying more frequently than that by default.
    private static final long REGISTRATION_MAX_RETRY_ATTEMPTS = 5;
    private static final Duration REGISTRATION_MIN_RETRY_DELAY = Duration.ofSeconds(1);
    private static final Duration REGISTRATION_MAX_RETRY_DELAY = Duration.ofSeconds(5);

    /**
     * Initializes a FluxCapacitor object. Package-private for unit test use.
     *
     * @param sfn    - The client that should be used for all Step Functions API calls
     * @param config - Config data used to configure FluxCapacitor behavior.
     */
    FluxCapacitorImpl(MetricRecorderFactory metricsFactory, SfnClient sfn, FluxCapacitorConfig config, Clock clock) {
        this.metricsFactory = metricsFactory;
        this.sfn = sfn;
        this.config = config;
        this.clock = clock;

        this.workflowsByClass = new HashMap<>();
        this.activitiesByName = new HashMap<>();
        this.workflowsByActivityName = new HashMap<>();
    }

    // package-private for test access.
    Map<Class<? extends Workflow>, Workflow> getWorkflowsByClass() {
        return workflowsByClass;
    }

    // package-private for test access.
    Map<String, WorkflowStep> getActivitiesByName() {
        return activitiesByName;
    }

    // package-private for test access.
    Map<String, Workflow> getWorkflowsByActivityName() {
        return workflowsByActivityName;
    }

    /**
     * Creates a FluxCapacitor object and does various bits of setup.
     * Intentionally package-private, only the Factory should be using the constructor.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param credentials    - A provider for the AWS credentials that should be used to call AWS APIs
     * @param config         - Configuration data for FluxCapacitor to use to configure itself
     */
    static FluxCapacitor create(MetricRecorderFactory metricsFactory, AwsCredentialsProvider credentials,
                                FluxCapacitorConfig config) {
        // We do our own retry/backoff logic so we can get decent metrics, so here we disable the SDK's defaults.
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .retryCondition(RetryCondition.none())
                .numRetries(0)
                .backoffStrategy(BackoffStrategy.none())
                .throttlingBackoffStrategy(BackoffStrategy.none())
                .build();

        // If an override config was provided, use it, and only use the above RetryPolicy
        // if the provided overrideConfig did not include its own RetryPolicy.
        ClientOverrideConfiguration overrideConfig = config.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder()
                    .retryPolicy(retryPolicy)
                    .build();
        } else if (overrideConfig.retryPolicy().isEmpty()) {
            overrideConfig = overrideConfig.toBuilder().retryPolicy(retryPolicy).build();
        }

        SfnClientBuilder builder = SfnClient.builder()
                .credentialsProvider(credentials)
                .region(Region.of(config.getAwsRegion()))
                .overrideConfiguration(overrideConfig);

        // if the user specified a custom endpoint, we'll use it;
        // otherwise the SDK will figure it out based on the region name.
        if (config.getSfnEndpoint() != null && !"".equals(config.getSfnEndpoint())) {
            builder.endpointOverride(URI.create(config.getSfnEndpoint()));
        }

        return new FluxCapacitorImpl(metricsFactory, builder.build(), config, Clock.systemUTC());
    }

    @Override
    public void initialize(List<Workflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("The specified workflow list must not be empty.");
        } else if (!workflowsByClass.isEmpty()) {
            throw new IllegalArgumentException("Flux is already initialized.");
        }

        populateMaps(workflows);
        registerActivities();
        registerWorkflows();
        initializePollers();
        startPeriodicWorkflows();
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return null;
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        return null;
    }

    @Override
    public RemoteWorkflowExecutor getRemoteWorkflowExecutor(String endpointId) {
        // For the regular FluxCapacitor SfnClient, we disabled all the retry logic
        // since we do our own for metrics purposes. However, the remote client is not used
        // for much, and we don't bother emitting metrics for it, so the defaults are fine.

        Function<String, RemoteSfnClientConfig> remoteConfigProvider = config.getRemoteSfnClientConfigProvider();
        if (remoteConfigProvider == null) {
            throw new IllegalStateException("Cannot create a remote workflow executor without a remote client config provider.");
        }
        RemoteSfnClientConfig remoteConfig = remoteConfigProvider.apply(endpointId);
        if (remoteConfig == null) {
            throw new IllegalStateException("Cannot create a remote workflow executor without any remote client config.");
        }

        AwsCredentialsProvider credentials = remoteConfig.getCredentials();
        if (credentials == null) {
            credentials = DefaultCredentialsProvider.create();
        }

        ClientOverrideConfiguration overrideConfig = remoteConfig.getClientOverrideConfiguration();
        if (overrideConfig == null) {
            overrideConfig = ClientOverrideConfiguration.builder().build();
        }

        SfnClientBuilder customSfn = SfnClient.builder().credentialsProvider(credentials)
                .region(Region.of(remoteConfig.getAwsRegion()))
                .overrideConfiguration(overrideConfig);
        if (remoteConfig.getSfnEndpoint() != null) {
            customSfn.endpointOverride(URI.create(remoteConfig.getSfnEndpoint()));
        }
        return new RemoteWorkflowExecutorImpl(clock, metricsFactory, workflowsByClass, customSfn.build(), config, remoteConfig);
    }

    @Override
    public void shutdown() {
        for (ExecutorService s : activityTaskPollerThreadsPerActivity.values()) {
            s.shutdown();
        }
        for (ExecutorService s : workerThreadsPerTaskList.values()) {
            s.shutdown();
        }
        periodicWorkflowScheduler.shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, activityTaskPollerThreadsPerActivity.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, workerThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        return periodicWorkflowScheduler.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private long awaitTerminationAndReturnRemainingMillis(long timeoutMillis,
                                                          Collection<? extends ExecutorService> executors)
            throws InterruptedException {
        Duration remaining = Duration.ofMillis(timeoutMillis);
        for (ExecutorService s : executors) {
            if (remaining.isNegative()) {
                return remaining.toMillis();
            }
            Instant start = Instant.now();
            if (!s.awaitTermination(remaining.toMillis(), TimeUnit.MILLISECONDS)) {
                return -1;
            }
            remaining = remaining.minus(Duration.between(start, Instant.now()));
        }
        return remaining.toMillis();
    }

    // Package-private for RemoteWorkflowExecutorImpl and unit test access
    static StartExecutionRequest buildStartWorkflowRequest(Workflow workflow, String awsRegion, String awsAccountId,
                                                           String executionId, Map<String, Object> input) {
        return StartExecutionRequest.builder()
                .input(StepAttributes.encode(StepAttributes.serializeMapValues(input)))
                .name(executionId)
                .stateMachineArn(SfnArnFormatter.workflowArn(awsRegion, awsAccountId, workflow.getClass()))
                .build();
    }

    // package-private for testing
    void populateMaps(List<Workflow> workflows) {
        Set<String> workflowNames = new HashSet<>();
        for (Workflow workflow : workflows) {
            IdentifierValidation.validateWorkflowName(workflow.getClass());

            String workflowName = TaskNaming.workflowName(workflow.getClass());
            if (workflowNames.contains(workflowName)) {
                String message = "Received more than one Workflow object with the same class name: " + workflowName;
                log.error(message);
                throw new FluxException(message);
            }
            workflowNames.add(workflowName);
            workflowsByClass.put(workflow.getClass(), workflow);

            for (Map.Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : workflow.getGraph().getNodes().entrySet()) {
                IdentifierValidation.validateStepName(entry.getKey());

                String activityName = buildSfnActivityName(workflow.getClass(), entry.getValue().getStep().getClass());
                if (activitiesByName.containsKey(activityName)) {
                    String message = "Workflow " + workflowName + " has two steps with the same name: "
                            + entry.getValue().getStep().getClass().getSimpleName();
                    log.error(message);
                    throw new FluxException(message);
                }
                activitiesByName.put(activityName, entry.getValue().getStep());
                workflowsByActivityName.put(activityName, workflow);
            }
        }
    }

    // package-private for test access
    String buildSfnActivityName(Class<? extends Workflow> wfClass, Class<? extends WorkflowStep> stepClass) {
        return String.format("%s-%s", wfClass.getSimpleName(), stepClass.getSimpleName());
    }

    // package-private for testing
    void registerActivities() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.RegisterActivities")) {
            Set<String> registeredActivities
                    = AwsRetryUtils.executeWithInlineBackoff(this::describeRegisteredActivities, REGISTRATION_MAX_RETRY_ATTEMPTS,
                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                    metrics, LIST_ACTIVITIES_METRIC_PREFIX);

            for (String activityName : activitiesByName.keySet()) {
                if (registeredActivities.contains(activityName)) {
                    log.info("Activity {} is already registered.", activityName);
                } else {
                    log.info("Registering activity {}", activityName);

                    CreateActivityRequest req = buildCreateActivityRequest(activityName);

                    AwsRetryUtils.executeWithInlineBackoff(() -> sfn.createActivity(req),
                            REGISTRATION_MAX_RETRY_ATTEMPTS,
                            REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                            metrics, CREATE_ACTIVITY_METRIC_PREFIX);
                    log.info("Successfully registered activity {}", activityName);
                }
            }
        }
    }

    private Set<String> describeRegisteredActivities() {
        ListActivitiesRequest request = ListActivitiesRequest.builder().build();
        return sfn.listActivitiesPaginator(request).activities().stream()
                .map(ActivityListItem::name)
                .collect(Collectors.toSet());
    }

    static CreateActivityRequest buildCreateActivityRequest(String activityName) {
        return CreateActivityRequest.builder()
                .name(activityName)
                .build();
    }

    // package-private for testing
    void registerWorkflows() {
        // TODO
    }

    // useless to unit-test this method, all it does is start a bunch of threads that immediately try to poll for work
    private void initializePollers() {
        String hostname;
        try {
            hostname = shortenHostnameForIdentity(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to determine hostname", e);
        }

        hostname = config.getHostnameTransformerForPollerIdentity().apply(hostname);

        IdentifierValidation.validateHostname(hostname);

        workerThreadsPerTaskList = new HashMap<>();
        Set<String> taskLists = workflowsByClass.values().stream().map(Workflow::taskList).collect(Collectors.toSet());
        for (String taskList : taskLists) {
            int poolSize = config.getTaskListConfig(taskList).getActivityTaskThreadCount();
            String poolName = String.format("worker-%s", taskList);
            workerThreadsPerTaskList.put(taskList, new BlockOnSubmissionThreadPoolExecutor(poolSize, poolName));
        }

        activityTaskPollerThreadsPerActivity = new HashMap<>();
        for (Map.Entry<String, WorkflowStep> entry : activitiesByName.entrySet()) {
            Workflow workflow = workflowsByActivityName.get(entry.getKey());
            WorkflowStep step = entry.getValue();
            String activityArn = SfnArnFormatter.activityArn(config.getAwsRegion(), config.getAwsAccountId(),
                                                             workflow.getClass(), step.getClass());
            ScheduledExecutorService service = createActivityPollerPool(entry.getKey(), hostname,
                    config.getTaskListConfig(workflow.taskList()).getActivityPollerThreadCount(),
                    workerName -> new ActivityTaskPoller(metricsFactory, sfn, workerName, activityArn,
                            workflow, step, workerThreadsPerTaskList.get(workflow.taskList())));
            activityTaskPollerThreadsPerActivity.put(entry.getKey(), service);
        }
    }

    /**
     * Creates a worker pool.
     *
     * @param activityName - The name of the activity this pool should poll for.
     * @param hostname     - The hostname of this worker.
     * @param poolSize     - The size of the pool
     * @param taskCreator  - A lambda that takes the full worker name as input and returns a new worker task to add to the pool.
     */
    private ScheduledExecutorService createActivityPollerPool(String activityName, String hostname, int poolSize,
                                                              Function<String, Runnable> taskCreator) {
        String poolName = String.format("poller-%s", activityName);
        ThreadFactory threadFactory = ThreadUtils.createStackTraceSuppressingThreadFactory(poolName);
        ScheduledExecutorService service = Executors.newScheduledThreadPool(poolSize, threadFactory);
        for (int i = 0; i < poolSize; i++) {
            // Since activity pollers are per-activity, the activity ARN is in the GetActivityTask request already.
            // So all we need to do is put the hostname and the thread number in the identity.
            String taskName = String.format("%s_%s", hostname, i);
            Runnable task = taskCreator.apply(taskName);
            // Scheduling the pollers so they will be restarted as soon as they end.
            service.scheduleWithFixedDelay(ThreadUtils.wrapInExceptionSwallower(task), 0, 10, TimeUnit.MILLISECONDS);
        }
        return service;
    }

    /**
     * Worker identity strings can't be too long so this method will trim out ".ec2.amazonaws.com" and similar.
     * Additional hostname transformations can be injected by setting
     * FluxCapacitorConfig's hostnameTransformerForPollerIdentity.
     *
     * Package-private for testing purposes.
     *
     * @param hostname The hostname to shorten
     * @return The shortened hostname
     */
    static String shortenHostnameForIdentity(String hostname) {
        String result = hostname.replace(".ec2.amazonaws.com.cn", "");
        result = result.replace(".ec2.amazonaws.com", "");
        result = result.replace(".compute.amazonaws.com.cn", "");
        result = result.replace(".compute.amazonaws.com", "");
        result = result.replace(".compute.internal", "");
        return result;
    }

    private void startPeriodicWorkflows() {
        periodicWorkflowScheduler = Executors.newScheduledThreadPool(1);
        // TODO
    }
}
