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

package software.amazon.aws.clients.swf.flux;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorder;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.poller.ActivityTaskPoller;
import software.amazon.aws.clients.swf.flux.poller.BlockOnSubmissionThreadPoolExecutor;
import software.amazon.aws.clients.swf.flux.poller.DecisionTaskPoller;
import software.amazon.aws.clients.swf.flux.poller.TaskNaming;
import software.amazon.aws.clients.swf.flux.step.StepAttributes;
import software.amazon.aws.clients.swf.flux.step.WorkflowStep;
import software.amazon.aws.clients.swf.flux.util.RetryUtils;
import software.amazon.aws.clients.swf.flux.util.ThreadUtils;
import software.amazon.aws.clients.swf.flux.wf.Periodic;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphNode;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.swf.SwfClient;
import software.amazon.awssdk.services.swf.SwfClientBuilder;
import software.amazon.awssdk.services.swf.model.ChildPolicy;
import software.amazon.awssdk.services.swf.model.DescribeDomainRequest;
import software.amazon.awssdk.services.swf.model.ListActivityTypesRequest;
import software.amazon.awssdk.services.swf.model.ListWorkflowTypesRequest;
import software.amazon.awssdk.services.swf.model.RegisterActivityTypeRequest;
import software.amazon.awssdk.services.swf.model.RegisterDomainRequest;
import software.amazon.awssdk.services.swf.model.RegisterWorkflowTypeRequest;
import software.amazon.awssdk.services.swf.model.RegistrationStatus;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionRequest;
import software.amazon.awssdk.services.swf.model.StartWorkflowExecutionResponse;
import software.amazon.awssdk.services.swf.model.TaskList;
import software.amazon.awssdk.services.swf.model.TypeAlreadyExistsException;
import software.amazon.awssdk.services.swf.model.UnknownResourceException;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionAlreadyStartedException;
import software.amazon.awssdk.services.swf.model.WorkflowExecutionInfo;
import software.amazon.awssdk.services.swf.model.WorkflowType;

/**
 * The primary class through which the Flux library is used at runtime.
 */
public final class FluxCapacitorImpl implements FluxCapacitor {

    public static final String WORKFLOW_VERSION = "1";
    public static final String WORKFLOW_EXECUTION_RETENTION_PERIOD_IN_DAYS = "90";
    public static final String DEFAULT_DECISION_TASK_TIMEOUT = "30";

    private static final String DESCRIBE_DOMAIN_METRIC_PREFIX = "Flux.DescribeDomain";
    private static final String REGISTER_DOMAIN_METRIC_PREFIX = "Flux.RegisterDomain";
    private static final String LIST_WORKFLOW_TYPES_METRIC_PREFIX = "Flux.ListWorkflowTypes";
    private static final String REGISTER_WORKFLOW_TYPE_METRIC_PREFIX = "Flux.RegisterWorkflowType";
    private static final String LIST_ACTIVITY_TYPES_METRIC_PREFIX = "Flux.ListActivityTypes";
    private static final String REGISTER_ACTIVITY_TYPE_METRIC_PREFIX = "Flux.RegisterActivityType";

    // The default throttling refill rate for the Register APIs is 1 per second in most regions,
    // so there's no sense retrying more frequently than that by default.
    private static final long REGISTRATION_MAX_RETRY_ATTEMPTS = 5;
    private static final Duration REGISTRATION_MIN_RETRY_DELAY = Duration.ofSeconds(1);
    private static final Duration REGISTRATION_MAX_RETRY_DELAY = Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(FluxCapacitorImpl.class);

    private final SwfClient swf;
    private final String workflowDomain;
    private final FluxCapacitorConfig config;

    private final Map<String, Workflow> workflowsByName;
    private final Map<String, WorkflowStep> activitiesByName;

    public static final double DEFAULT_EXPONENTIAL_BACKOFF_BASE = 1.25;

    private Map<String, ScheduledExecutorService> decisionTaskPollerThreadsPerTaskList;
    private Map<String, BlockOnSubmissionThreadPoolExecutor> deciderThreadsPerTaskList;
    private Map<String, ScheduledExecutorService> activityTaskPollerThreadsPerTaskList;
    private Map<String, BlockOnSubmissionThreadPoolExecutor> workerThreadsPerTaskList;
    private Map<String, ScheduledExecutorService> periodicThreadsPerTaskList;

    private final MetricRecorderFactory metricsFactory;

    // This is only used to make a best-effort attempt to spread workflow executions across
    // task list buckets. It does not need to be secure nor a perfect distribution.
    private static final Random RANDOM = new Random();

    private final Clock clock;

    /**
     * Creates a FluxCapacitor object and does various bits of setup e.g. registering the swf domain.
     * Intentionally package-private, only the Factory should be using the constructor.
     *
     * @param metricsFactory - A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param credentials    - A provider for the AWS credentials that should be used to call SWF APIs
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
        } else if (!overrideConfig.retryPolicy().isPresent()) {
            overrideConfig = overrideConfig.toBuilder().retryPolicy(retryPolicy).build();
        }

        SwfClientBuilder builder = SwfClient.builder()
                .credentialsProvider(credentials)
                .region(Region.of(config.getAwsRegion()))
                .overrideConfiguration(overrideConfig);

        // if the user specified a custom endpoint, we'll use it;
        // otherwise the SDK will figure it out based on the region name.
        if (config.getSwfEndpoint() != null && !"".equals(config.getSwfEndpoint())) {
            builder.endpointOverride(URI.create(config.getSwfEndpoint()));
        }

        return new FluxCapacitorImpl(metricsFactory, builder.build(), config, Clock.systemUTC());
    }

    @Override
    public void initialize(List<Workflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("The specified workflow list must not be empty.");
        } else if (!workflowsByName.isEmpty()) {
            throw new IllegalArgumentException("Flux is already initialized.");
        }

        populateNameMaps(workflows);
        ensureDomainExists();
        registerWorkflows();
        registerActivities();
        initializePollers();
        startPeriodicWorkflows();
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput) {
        return executeWorkflow(workflowType, workflowId, workflowInput, Collections.emptySet());
    }

    @Override
    public WorkflowStatusChecker executeWorkflow(Class<? extends Workflow> workflowType, String workflowId,
                                                 Map<String, Object> workflowInput, Set<String> executionTags) {
        if (workflowsByName.isEmpty()) {
            throw new WorkflowExecutionException("Flux has not yet been initialized, please call the initialize method first.");
        }

        String workflowName = TaskNaming.workflowName(workflowType);
        if (!workflowsByName.containsKey(workflowName)) {
            throw new WorkflowExecutionException("Cannot execute a workflow that was not provided to Flux at initialization: "
                                                 + workflowName);
        }

        Workflow workflow = workflowsByName.get(workflowName);

        Set<String> actualExecutionTags = new HashSet<>(executionTags);
        // if the auto-tagging config is not set, it's enabled by default
        if (config.getAutomaticallyTagExecutionsWithTaskList() == null
            || config.getAutomaticallyTagExecutionsWithTaskList()) {
            actualExecutionTags.add(workflow.taskList());
        }

        // nextInt generates a number between 0 (inclusive) and bucketCount (exclusive).
        // We want a number between 1 (inclusive) and bucketCount (inclusive), so we can just add one to the result.
        int bucket = 1 + RANDOM.nextInt(config.getTaskListConfig(workflow.taskList()).getBucketCount());
        String taskList = synthesizeBucketedTaskListName(workflow.taskList(), bucket);

        StartWorkflowExecutionRequest request = buildStartWorkflowRequest(workflowDomain, workflowName, workflowId,
                                                                          taskList, workflow.maxStartToCloseDuration(),
                                                                          workflowInput, actualExecutionTags);

        log.debug("Requesting new workflow execution for workflow {} with id {}", workflowName, workflowId);

        try {
            StartWorkflowExecutionResponse response = swf.startWorkflowExecution(request);
            log.debug("Started workflow {} with id {}: received execution id {}.", workflowName, workflowId, response.runId());
            return new WorkflowStatusCheckerImpl(swf, workflowDomain, workflowId, response.runId());
        } catch (WorkflowExecutionAlreadyStartedException e) {
            log.debug("Attempted to start workflow {} with id {} but it was already started.", workflowName, workflowId, e);
            // swallow, we're ok with this happening

            // TODO -- figure out how to find the run id so we can return a useful WorkflowStatusChecker
            return new WorkflowStatusChecker() {
                @Override
                public WorkflowStatus checkStatus() {
                    return WorkflowStatus.UNKNOWN;
                }

                @Override
                public WorkflowExecutionInfo getExecutionInfo() {
                    return null;
                }

                public SwfClient getSwfClient() {
                    return swf;
                }
            };
        } catch (Exception e) {
            String message = String.format("Got exception attempting to start workflow %s with id %s",
                                           workflowName, workflowId);
            log.debug(message, e);
            throw new WorkflowExecutionException(message, e);
        }
    }

    @Override
    public RemoteWorkflowExecutor getRemoteWorkflowExecutor(String swfRegion, String swfEndpoint,
                                                            AwsCredentialsProvider credentials, String workflowDomain) {
        // for the regular FluxCapacitor SwfClient, we disabled all the retry logic
        // since we do our own for metrics purposes. However the remote client is not used
        // for much, and we don't bother emitting metrics for it, so the defaults are fine.

        // this is where we'd add an execution interceptor for e.g. SDK metrics
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder().build();

        SwfClientBuilder customSwf = SwfClient.builder().credentialsProvider(credentials)
                .region(Region.of(swfRegion))
                .overrideConfiguration(overrideConfig);
        if (swfEndpoint != null) {
            customSwf.endpointOverride(URI.create(swfEndpoint));
        }
        return new RemoteWorkflowExecutorImpl(metricsFactory, workflowsByName, customSwf.build(), config);
    }

    @Override
    public void shutdown() {
        for (ExecutorService s : decisionTaskPollerThreadsPerTaskList.values()) {
            s.shutdown();
        }
        for (ExecutorService s : activityTaskPollerThreadsPerTaskList.values()) {
            s.shutdown();
        }
        for (ExecutorService s : deciderThreadsPerTaskList.values()) {
            s.shutdown();
        }
        for (ExecutorService s : workerThreadsPerTaskList.values()) {
            s.shutdown();
        }
        for (ExecutorService s : periodicThreadsPerTaskList.values()) {
            s.shutdown();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, decisionTaskPollerThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, activityTaskPollerThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, deciderThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, workerThreadsPerTaskList.values());
        if (timeoutMillis < 0) {
            return false;
        }
        timeoutMillis = awaitTerminationAndReturnRemainingMillis(timeoutMillis, periodicThreadsPerTaskList.values());
        return timeoutMillis >= 0;
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

    // visible for use by unit tests
    static StartWorkflowExecutionRequest buildStartWorkflowRequest(String workflowDomain, String workflowName,
                                                                   String workflowId, String taskList,
                                                                   Duration startToCloseDuration, Map<String, ?> input,
                                                                   Set<String> executionTags) {
        // yes, this means the input attributes are serialized into a string map, which is itself serialized.
        // this makes deserialization less confusing later because we can deserialize as a map of strings
        // and then deserialize each value as a specific type.
        String rawInput = StepAttributes.encode(StepAttributes.serializeMapValues(input));
        WorkflowType workflowType = WorkflowType.builder().name(workflowName).version(WORKFLOW_VERSION).build();
        return StartWorkflowExecutionRequest.builder()
                                            .domain(workflowDomain)
                                            .workflowType(workflowType)
                                            .childPolicy(ChildPolicy.TERMINATE)
                                            .taskList(TaskList.builder().name(taskList).build())
                                            .workflowId(workflowId)
                                            .input(rawInput)
                                            .taskStartToCloseTimeout(DEFAULT_DECISION_TASK_TIMEOUT)
                                            .executionStartToCloseTimeout(Long.toString(startToCloseDuration.getSeconds()))
                                            .tagList(executionTags)
                                            .build();
    }

    /**
     * Initializes a FluxCapacitor object. Package-private for unit test use.
     *
     * @param swf    - The SWF client that should be used for all SWF API calls
     * @param config - Config data used to configure FluxCapacitor behavior.
     */
    FluxCapacitorImpl(MetricRecorderFactory metricsFactory, SwfClient swf, FluxCapacitorConfig config, Clock clock) {
        this.metricsFactory = metricsFactory;
        this.swf = swf;
        this.config = config;
        this.workflowDomain = config.getSwfDomain();
        this.clock = clock;

        this.workflowsByName = new HashMap<>();
        this.activitiesByName = new HashMap<>();
    }

    // package-private for access during unit tests
    void populateNameMaps(List<Workflow> workflows) {
        for (Workflow workflow : workflows) {
            String workflowName = TaskNaming.workflowName(workflow.getClass());
            if (workflowsByName.containsKey(workflowName)) {
                String message = "Received more than one Workflow object with the same class name: " + workflowName;
                log.error(message);
                throw new RuntimeException(message);
            }
            workflowsByName.put(workflowName, workflow);

            for (Entry<Class<? extends WorkflowStep>, WorkflowGraphNode> entry : workflow.getGraph().getNodes().entrySet()) {
                String activityName = TaskNaming.activityName(workflowName, entry.getValue().getStep());
                if (activitiesByName.containsKey(activityName)) {
                    String message = "Workflow " + workflowName + " has two steps with the same name: " + activityName;
                    log.error(message);
                    throw new RuntimeException(message);
                }
                activitiesByName.put(activityName, entry.getValue().getStep());
            }
        }
    }

    // package-private for testing
    void ensureDomainExists() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.EnsureDomainExists")) {
            try {
                log.info("Looking up SWF domain {} to see if it needs to be registered.", workflowDomain);

                DescribeDomainRequest request = DescribeDomainRequest.builder().name(workflowDomain).build();
                RetryUtils.executeWithInlineBackoff(() -> swf.describeDomain(request), REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                    metrics, DESCRIBE_DOMAIN_METRIC_PREFIX);

                log.info("SWF domain {} is already registered.", workflowDomain);
            } catch (UnknownResourceException e) {
                log.info("Registering SWF domain {}", workflowDomain);

                RegisterDomainRequest request = RegisterDomainRequest.builder().name(workflowDomain)
                        .workflowExecutionRetentionPeriodInDays(WORKFLOW_EXECUTION_RETENTION_PERIOD_IN_DAYS)
                        .build();
                RetryUtils.executeWithInlineBackoff(() -> swf.registerDomain(request), REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                    REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                    metrics, REGISTER_DOMAIN_METRIC_PREFIX);

                log.info("Successfully registered SWF domain {}", workflowDomain);
            }
        }
    }

    // package-private for testing
    void registerWorkflows() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.RegisterWorkflows")) {
            Set<String> registeredWorkflows
                    = RetryUtils.executeWithInlineBackoff(this::describeRegisteredWorkflows, REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                          REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                          metrics, LIST_WORKFLOW_TYPES_METRIC_PREFIX);
            for (String workflowName : workflowsByName.keySet()) {
                if (registeredWorkflows.contains(workflowName)) {
                    log.info("Workflow {} is already registered.", workflowName);
                } else {
                    log.info("Registering workflow {}", workflowName);

                    RegisterWorkflowTypeRequest regReq = buildRegisterWorkflowRequest(workflowDomain, workflowName);

                    try {
                        RetryUtils.executeWithInlineBackoff(() -> swf.registerWorkflowType(regReq), REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                            REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                            metrics, REGISTER_WORKFLOW_TYPE_METRIC_PREFIX);
                        log.info("Successfully registered workflow {}", workflowName);
                    } catch (TypeAlreadyExistsException ex) {
                        log.info("Attempted to register workflow {} but someone else did it first.", workflowName);
                        // ignore, it's ok
                    } // propagate any other service exception
                }
            }
        }
    }

    private Set<String> describeRegisteredWorkflows() {
        ListWorkflowTypesRequest request = ListWorkflowTypesRequest.builder().domain(workflowDomain)
                                                                   .registrationStatus(RegistrationStatus.REGISTERED)
                                                                   .build();
        return swf.listWorkflowTypesPaginator(request).typeInfos().stream()
                                                                  .map(typeInfo -> typeInfo.workflowType().name())
                                                                  .collect(Collectors.toSet());
    }

    // package-private for unit test use
    static RegisterWorkflowTypeRequest buildRegisterWorkflowRequest(String workflowDomain, String workflowName) {
        long timeout = Workflow.WORKFLOW_EXECUTION_DEFAULT_START_TO_CLOSE_TIMEOUT.getSeconds();
        return RegisterWorkflowTypeRequest.builder().domain(workflowDomain)
                .name(workflowName)
                .version(WORKFLOW_VERSION)
                .defaultTaskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
                .defaultExecutionStartToCloseTimeout(Long.toString(timeout))
                .defaultTaskStartToCloseTimeout(DEFAULT_DECISION_TASK_TIMEOUT)
                .defaultChildPolicy(ChildPolicy.TERMINATE)
                .build();
    }

    // package-private for testing
    void registerActivities() {
        try (MetricRecorder metrics = metricsFactory.newMetricRecorder("Flux.RegisterActivities")) {
            Set<String> registeredActivities
                    = RetryUtils.executeWithInlineBackoff(this::describeRegisteredActivities, REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                          REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                          metrics, LIST_ACTIVITY_TYPES_METRIC_PREFIX);

            for (Entry<String, WorkflowStep> entry : activitiesByName.entrySet()) {
                String activityName = entry.getKey();

                if (registeredActivities.contains(activityName)) {
                    log.info("Activity {} is already registered.", activityName);
                } else {
                    log.info("Registering activity {}", activityName);

                    RegisterActivityTypeRequest regReq = buildRegisterActivityRequest(workflowDomain, activityName);

                    try {
                        RetryUtils.executeWithInlineBackoff(() -> swf.registerActivityType(regReq), REGISTRATION_MAX_RETRY_ATTEMPTS,
                                                            REGISTRATION_MIN_RETRY_DELAY, REGISTRATION_MAX_RETRY_DELAY,
                                                            metrics, REGISTER_ACTIVITY_TYPE_METRIC_PREFIX);
                        log.info("Successfully registered activity {}", activityName);
                    } catch (TypeAlreadyExistsException ex) {
                        log.info("Attempted to register activity {} but someone else did it first.", activityName);
                        // ignore, it's ok
                    } // propagate any other service exception
                }
            }
        }
    }

    private Set<String> describeRegisteredActivities() {
        ListActivityTypesRequest request = ListActivityTypesRequest.builder().domain(workflowDomain)
                                                                             .registrationStatus(RegistrationStatus.REGISTERED)
                                                                             .build();
        return swf.listActivityTypesPaginator(request).typeInfos().stream()
                                                                  .map(typeInfo -> typeInfo.activityType().name())
                                                                  .collect(Collectors.toSet());
    }

    // visible for unit test use
    static RegisterActivityTypeRequest buildRegisterActivityRequest(String workflowDomain, String activityName) {
        return RegisterActivityTypeRequest.builder().domain(workflowDomain)
                .name(activityName)
                .version(WORKFLOW_VERSION)
                .defaultTaskList(TaskList.builder().name(Workflow.DEFAULT_TASK_LIST_NAME).build())
                .defaultTaskScheduleToStartTimeout("NONE")
                .defaultTaskStartToCloseTimeout("NONE")
                .defaultTaskScheduleToCloseTimeout("NONE")
                .defaultTaskHeartbeatTimeout("60")
                .build();
    }

    // useless to unit-test this method, all it does is start a bunch of threads that immediately try to poll SWF
    private void initializePollers() {
        String hostname;
        try {
            hostname = shortenHostnameForIdentity(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to determine hostname", e);
        }

        hostname = config.getHostnameTransformerForPollerIdentity().apply(hostname);

        decisionTaskPollerThreadsPerTaskList = new HashMap<>();
        deciderThreadsPerTaskList = new HashMap<>();
        activityTaskPollerThreadsPerTaskList = new HashMap<>();
        workerThreadsPerTaskList = new HashMap<>();

        final double exponentialBackoffCoefficient = (config.getExponentialBackoffBase() != null
                                                        ? config.getExponentialBackoffBase()
                                                        : DEFAULT_EXPONENTIAL_BACKOFF_BASE);

        Set<String> baseTaskLists = workflowsByName.values().stream().map(Workflow::taskList).collect(Collectors.toSet());
        Set<String> bucketedTaskLists = new HashSet<>(baseTaskLists);
        for (String taskList : baseTaskLists) {
            // We start at 2 here since we already added the base task list names to the set,
            // and bucket 1 is the base name with no suffix. See synthesizeBucketedTaskListName.
            for (int i = 2; i <= config.getTaskListConfig(taskList).getBucketCount(); i++) {
                bucketedTaskLists.add(synthesizeBucketedTaskListName(taskList, i));
            }
        }

        for (String taskList : bucketedTaskLists) {
            int poolSize = config.getTaskListConfig(taskList).getDecisionTaskThreadCount();
            String poolName = String.format("%s-%s", "decider", taskList);
            deciderThreadsPerTaskList.put(taskList, new BlockOnSubmissionThreadPoolExecutor(poolSize, poolName));

            poolSize = config.getTaskListConfig(taskList).getDecisionTaskPollerThreadCount();
            ScheduledExecutorService service = createExecutorService(taskList, hostname, "decisionPoller", poolSize,
                deciderName -> new DecisionTaskPoller(metricsFactory, swf, workflowDomain, taskList, deciderName,
                                                      exponentialBackoffCoefficient, workflowsByName,
                                                      activitiesByName, deciderThreadsPerTaskList.get(taskList),
                                                      clock));
            decisionTaskPollerThreadsPerTaskList.put(taskList, service);

            poolSize = config.getTaskListConfig(taskList).getActivityTaskThreadCount();
            poolName = String.format("%s-%s", "activityWorker", taskList);
            workerThreadsPerTaskList.put(taskList, new BlockOnSubmissionThreadPoolExecutor(poolSize, poolName));

            poolSize = config.getTaskListConfig(taskList).getActivityTaskPollerThreadCount();
            service = createExecutorService(taskList, hostname, "activityPoller", poolSize,
                workerName -> new ActivityTaskPoller(metricsFactory, swf, workflowDomain, taskList, workerName,
                                                     workflowsByName, activitiesByName,
                                                     workerThreadsPerTaskList.get(taskList)));
            activityTaskPollerThreadsPerTaskList.put(taskList, service);
        }
    }

    /**
     * We don't suffix the first bucket name with the bucket number so that users can simply
     * increase the bucket count and roll out the change. Otherwise, the new code would stop polling on
     * the non-suffixed task list, potentially leaving workflows running with nothing polling for the work.
     *
     * Visible for testing.
     */
    static String synthesizeBucketedTaskListName(String baseTaskListName, int bucketNumber) {
        if (bucketNumber == 1) {
            return baseTaskListName;
        }
        return String.format("%s-%s", baseTaskListName, bucketNumber);
    }

    /**
     * Creates a worker pool.
     * @param taskList       - The task list this pool should poll for.
     * @param hostname       - The hostname of this worker.
     * @param taskTypeName   - A prefix for the worker names, e.g. "decider" or "worker"
     * @param poolSize       - The size of the pool
     * @param taskCreator    - A lambda that takes the full worker name as input and returns a new worker task to add to the pool.
     */
    private ScheduledExecutorService createExecutorService(String taskList, String hostname, String taskTypeName, int poolSize,
                                                           Function<String, Runnable> taskCreator) {
        String poolName = String.format("%s-%s", taskTypeName, taskList);
        ThreadFactory threadFactory = ThreadUtils.createStackTraceSuppressingThreadFactory(poolName);
        ScheduledExecutorService service = Executors.newScheduledThreadPool(poolSize, threadFactory);
        for (int i = 0; i < poolSize; i++) {
            String taskName = String.format("%s_%s-%s_%s", hostname, taskTypeName, taskList, i);
            Runnable task = taskCreator.apply(taskName);
            // Scheduling the pollers so they will be restarted as soon as they end.
            service.scheduleWithFixedDelay(ThreadUtils.wrapInExceptionSwallower(task), 0, 10, TimeUnit.MILLISECONDS);
        }
        return service;
    }

    /**
     * SWF worker identity strings can't be too long so this method will trim out ".ec2.amazonaws.com" and similar.
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

    // useless to unit-test this method, all it does is start a bunch of threads that immediately try to start a bunch of workflows
    private void startPeriodicWorkflows() {
        periodicThreadsPerTaskList = new HashMap<>();

        for (Entry<String, Workflow> entry : workflowsByName.entrySet()) {
            Workflow workflow = entry.getValue();
            if (!isPeriodicWorkflow(workflow)) {
                continue;
            }

            String taskList = workflow.taskList();
            if (!periodicThreadsPerTaskList.containsKey(taskList)) {
                int poolSize = config.getTaskListConfig(taskList).getPeriodicSubmitterThreadCount();
                periodicThreadsPerTaskList.put(workflow.taskList(), Executors.newScheduledThreadPool(poolSize));
            }

            log.debug(String.format("Scheduling periodic runs for workflow %s", entry.getKey()));
            String workflowId = TaskNaming.workflowName(workflow.getClass());
            Runnable runPeriodic = () -> executeWorkflow(workflow.getClass(), workflowId, new HashMap<>());

            Periodic period = workflow.getClass().getAnnotation(Periodic.class);
            long intervalSeconds = TimeUnit.SECONDS.convert(period.runInterval(), period.intervalUnits());

            // clamp the interval to the range [5s, 3600s] inclusive, but we'll aim for half of the workflow's period.
            intervalSeconds = Math.min(Math.max(5, intervalSeconds / 2), TimeUnit.SECONDS.convert(1, TimeUnit.HOURS));

            // We attempt to start the periodic workflows every intervalSeconds to ensure that they'll always
            // get executed either immediately upon startup, or up to intervalSeconds after being manually terminated.
            // SWF will dedupe by workflow id so we don't need to worry about extras running.
            periodicThreadsPerTaskList.get(taskList).scheduleAtFixedRate(runPeriodic, 0, intervalSeconds, TimeUnit.SECONDS);
        }
    }

    /* Package-private for testing */
    static boolean isPeriodicWorkflow(Workflow workflow) {
        return workflow.getClass().isAnnotationPresent(Periodic.class);
    }
}
