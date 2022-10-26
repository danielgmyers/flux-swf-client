
Library initialization with Guice
------------------------------------

You'll need a dependency on `flux-guice` to make use of this example config.

First, add a `FluxModule` object to your injector, where "example.flux.workflows" is the package containing your workflows. FluxModule will automatically find any classes under that package (recursively) of type `com.danielgmyers.flux.wf.Workflow` and initialize them with the Guice injector as singletons. You can add `@Inject` to your Workflow class constructors and they will be initialized as expected.

```java
package example.flux;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.guice.FluxModule;
import example.flux.MyApplicationModule;

public class MyApplication {

    public static void main() {

        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                                                 new MyApplicationModule(),
                                                 new FluxModule("example.flux.workflows")
        );

        FluxCapacitor flux = injector.getInstance(FluxCapacitor.class);

        // wait for termination
        Thread.currentThread().join();

        // shut down the workers, then given them 30 seconds to flush any quick in-flight steps.
        // steps that take longer will just have to get killed and restarted.
        flux.shutdown();
        flux.awaitTermination(30, TimeUnit.SECONDS);
    }
}
```

Next, provide the required configuration in your application module (either via binding or `@Provides`). This example code uses both approaches, to demonstrate.

```java
package example.flux;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import com.danielgmyers.flux.clients.swf.guice.SwfRegion;
import com.danielgmyers.flux.clients.swf.guice.WorkflowDomain;
import com.danielgmyers.flux.clients.swf.metrics.MetricRecorderFactory;
import com.danielgmyers.flux.clients.swf.metrics.NoopMetricRecorderFactory;

public class MyApplicationModule extends AbstractModule {

    @Override
    protected void configure() {
        // These specify the AWS Region and SWF workflow domain that Flux will use to register and execute workflows.
        bindConstant().annotatedWith(SwfRegion.class).to("us-west-2");
        bindConstant().annotatedWith(WorkflowDomain.class).to("workflow-domain");
    }

    @Provides
    @Singleton
    AwsCredentialsProvider fluxCredentials() {
        // FluxModule expects an AwsCredentialsProvider to be available for injection.
        return DefaultCredentialsProvider.builder().build();
    }

    @Provides
    @Singleton
    MetricRecorderFactory fluxMetricRecorderFactory() {
        // FluxModule expects a MetricRecorderFactory to be available for injection.
        // If you don't have a custom metric recorder you can use the default Noop recorder factory.
        return new NoopMetricRecorderFactory();
    }

}
```