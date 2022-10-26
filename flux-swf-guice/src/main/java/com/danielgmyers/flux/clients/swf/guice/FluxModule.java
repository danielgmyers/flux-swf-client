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

package com.danielgmyers.flux.clients.swf.guice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.swf.TaskListConfig;
import com.danielgmyers.flux.wf.Workflow;
import com.danielgmyers.metrics.MetricRecorderFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

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
        try (ScanResult scanResult = new ClassGraph().enableClassInfo().acceptPackages(workflowClasspath).scan()) {
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
    public void initializeFlux(FluxCapacitor fluxCapacitor, Set<Workflow> workflows) {
        fluxCapacitor.initialize(new ArrayList<>(workflows));
    }

    /**
     * Creates a FluxCapacitorConfig from configuration in the Guice context.
     */
    @Provides
    @Singleton
    public FluxCapacitorConfig getFluxCapacitorConfig(@SwfRegion String swfRegion,
                                                      @WorkflowDomain String workflowDomain,
                                                      FluxOptionalConfigHolder configHolder) {
        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion(swfRegion);
        config.setSwfDomain(workflowDomain);
        if (configHolder.getSwfEndpoint() != null) {
            config.setSwfEndpoint(configHolder.getSwfEndpoint());
        }
        if (configHolder.getExponentialBackoffBase() != null) {
            config.setExponentialBackoffBase(configHolder.getExponentialBackoffBase());
        }
        if (configHolder.getAutomaticallyTagExecutionsWithTaskList() != null) {
            config.setAutomaticallyTagExecutionsWithTaskList(configHolder.getAutomaticallyTagExecutionsWithTaskList());
        }
        applyTaskListConfigData(config, configHolder.getTaskListBucketCounts(),
                                TaskListConfig::setBucketCount);
        applyTaskListConfigData(config, configHolder.getTaskListActivityThreadCounts(),
                                TaskListConfig::setActivityTaskThreadCount);
        applyTaskListConfigData(config, configHolder.getTaskListActivityPollerThreadCounts(),
                                TaskListConfig::setActivityTaskPollerThreadCount);
        applyTaskListConfigData(config, configHolder.getTaskListDeciderThreadCounts(),
                                TaskListConfig::setDecisionTaskThreadCount);
        applyTaskListConfigData(config, configHolder.getTaskListDeciderPollerThreadCounts(),
                                TaskListConfig::setDecisionTaskPollerThreadCount);
        applyTaskListConfigData(config, configHolder.getTaskListPeriodicSubmitterThreadCounts(),
                                TaskListConfig::setPeriodicSubmitterThreadCount);
        return config;
    }

    private void applyTaskListConfigData(FluxCapacitorConfig config, Map<String, Integer> taskListData,
                                         BiConsumer<TaskListConfig, Integer> taskListConfigConsumer) {
        if (taskListData != null) {
            for (Map.Entry<String, Integer> entry : taskListData.entrySet()) {
                taskListConfigConsumer.accept(config.getTaskListConfig(entry.getKey()), entry.getValue());
            }
        }
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
