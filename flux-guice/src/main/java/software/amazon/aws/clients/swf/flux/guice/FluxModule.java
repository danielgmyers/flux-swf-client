package software.amazon.aws.clients.swf.flux.guice;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

import software.amazon.aws.clients.swf.flux.FluxCapacitor;
import software.amazon.aws.clients.swf.flux.FluxCapacitorConfig;
import software.amazon.aws.clients.swf.flux.FluxCapacitorFactory;
import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Usage: When setting up your Guice injection context,
 * create a FluxModule object and pass it to either Guice.createInjector()
 * or to install() while creating another Module.
 */
public class FluxModule extends AbstractModule {

    private final String workflowClasspath;

    /**
     * Constructs a FluxModule, and configures it to search the specified
     * java package on the classpath for classes that implement Workflow.
     */
    public FluxModule(String workflowClasspath) {
        this.workflowClasspath = workflowClasspath;
    }

    @VisibleForTesting
    List<Class<? extends Workflow>> findWorkflowClassesFromClasspath() {
        List<Class<? extends Workflow>> classes = new ArrayList<>();
        try (ScanResult scanResult = new ClassGraph().enableClassInfo().whitelistPackages(workflowClasspath).scan()) {
            for (ClassInfo workflow : scanResult.getClassesImplementing(Workflow.class.getCanonicalName())) {
                classes.add(workflow.loadClass(Workflow.class));
            }
        }
        return classes;
    }

    @Override
    protected void configure() {
        Multibinder<Workflow> wfBinder = Multibinder.newSetBinder(binder(), Workflow.class);
        findWorkflowClassesFromClasspath().forEach(wf -> wfBinder.addBinding().to(wf));

        // we do this so that the initializeFlux() method (below) gets called after
        // both the Workflow objects and the FluxCapacitor are initialized.
        // This prevents circular dependencies since Workflows may use the FluxCapacitor.
        requestInjection(this);
    }

    /**
     * Initializes the FluxCapacitor once it and the Workflow objects have been created.
     */
    @Inject
    public void intializeFlux(FluxCapacitor fluxCapacitor, Set<Workflow> workflows) {
        fluxCapacitor.initialize(new ArrayList<>(workflows));
    }

    /**
     * Creates a FluxCapacitorConfig from configuration in the Guice context.
     */
    @Provides
    @Singleton
    public FluxCapacitorConfig getFluxCapacitorConfig(@SwfRegion String swfRegion,
                                                      @WorkflowDomain String workflowDomain,
                                                      FluxOptionalConfigHolder optionalConfigHolder) {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(swfRegion);
        config.setSwfDomain(workflowDomain);
        if (optionalConfigHolder.getSwfEndpoint() != null) {
            config.setSwfEndpoint(optionalConfigHolder.getSwfEndpoint());
        }
        if (optionalConfigHolder.getExponentialBackoffBase() != null) {
            config.setExponentialBackoffBase(optionalConfigHolder.getExponentialBackoffBase());
        }
        config.setTaskListWorkerThreadCount(optionalConfigHolder.getTaskListActivityThreadCounts());
        config.setTaskListActivityTaskPollerThreadCount(optionalConfigHolder.getTaskListActivityPollerThreadCounts());
        config.setTaskListDeciderThreadCount(optionalConfigHolder.getTaskListDeciderThreadCounts());
        config.setTaskListDecisionTaskPollerThreadCount(optionalConfigHolder.getTaskListDeciderPollerThreadCounts());
        config.setTaskListPeriodicSubmitterThreadCount(optionalConfigHolder.getTaskListPeriodicSubmitterThreadCounts());
        return config;
    }

    /**
     * Creates a FluxCapacitor.
     */
    @Provides
    @Singleton
    public FluxCapacitor getFluxCapacitor(MetricRecorderFactory metricRecorderFactory,
                                          AwsCredentialsProvider awsCredentialsProvider,
                                          FluxCapacitorConfig fluxCapacitorConfig) {
        return FluxCapacitorFactory.create(metricRecorderFactory,
                                           awsCredentialsProvider,
                                           fluxCapacitorConfig);
    }
}
