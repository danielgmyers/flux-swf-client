package software.amazon.aws.clients.swf.flux.spring;

import org.springframework.beans.factory.annotation.Autowired;

import software.amazon.aws.clients.swf.flux.FluxCapacitor;
import software.amazon.aws.clients.swf.flux.FluxCapacitorConfig;
import software.amazon.aws.clients.swf.flux.FluxCapacitorFactory;

import software.amazon.aws.clients.swf.flux.metrics.MetricRecorderFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Helper for setting up a FluxCapacitor object in a spring context.
 */
public final class FluxSpringCreator {

    private static FluxCapacitor fc;

    private final MetricRecorderFactory metricsFactory;
    private final AwsCredentialsProvider creds;
    private final FluxCapacitorConfig config;

    /**
     * Creates a FluxSpringCreator object.
     * Create a bean for it, then create a FluxCapacitor bean using FluxSpringCreator.create as a factory method.
     * Finally, create a FluxSpringInitializer bean. You should be able to autowire all of them as long as
     * there are beans in your context with the @WorkflowRegion, @WorkflowEndpoint, and @WorkflowDomain qualifiers.
     *
     * @param metricsFactory A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param creds An AWSCredentialsProvider which will be used with the SWF client.
     * @param swfRegion The AWS region hosting the SWF endpoint the library should use.
     * @param swfEndpoint A specific SWF endpoint that the library should use.
     * @param swfDomain The SWF workflow domain that the library should use when registering and executing workflows.
     */
    @Autowired
    public FluxSpringCreator(MetricRecorderFactory metricsFactory, AwsCredentialsProvider creds, @WorkflowRegion String swfRegion,
                             @WorkflowEndpoint String swfEndpoint, @WorkflowDomain String swfDomain) {
        this.metricsFactory = metricsFactory;
        this.creds = creds;
        config = new FluxCapacitorConfig();
        config.setSwfDomain(swfDomain);
        config.setSwfEndpoint(swfEndpoint);
        config.setAwsRegion(swfRegion);
    }

    private FluxSpringCreator(MetricRecorderFactory metricsFactory, AwsCredentialsProvider creds, FluxCapacitorConfig config) {
        this.metricsFactory = metricsFactory;
        this.creds = creds;
        this.config = config;
    }

    /**
     * Creates a FluxSpringCreator object.
     * Create a bean for it, then create a FluxCapacitor bean using FluxSpringCreator.create as a factory method.
     *
     * @param metricsFactory A factory that produces MetricRecorder objects for emitting workflow metrics.
     * @param creds An AWSCredentialsProvider which will be used with the SWF client.
     * @param config Configuration data to be used by Flux to modify its behavior.
     */
    public static FluxSpringCreator createWithConfig(MetricRecorderFactory metricsFactory, AwsCredentialsProvider creds,
                                                     FluxCapacitorConfig config) {
        return new FluxSpringCreator(metricsFactory, creds, config);
    }

    /**
     * Returns a FluxCapacitor object using the configuration data in the spring context at
     * the time the FluxSpringCreator bean was created.
     * If called more than once, it returns the same object that was created the first time,
     * to ensure we never have multiple FluxCapacitor objects running at once with the same config.
     * @return A FluxCapacitor object
     */
    public synchronized FluxCapacitor create() {
        // This should only happen once, as this factory method should only be called once,
        // but we want to be sure we never have more than one FluxCapacitor in any given spring context,
        // so we'll be paranoid here and make sure. That's why this method is synchronized, too.
        if (fc == null) {
            fc = FluxCapacitorFactory.create(metricsFactory, creds, config);
        }
        return fc;
    }
}
