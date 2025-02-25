
Library initialization with Spring
------------------------------------

You'll need a dependency on `flux-swf-spring` to make use of this example config.

If you're using Spring, it is recommended to initialize your Workflow objects as singleton beans, either via component-scan or xml configuration.

```xml
<beans>
    <!-- This assumes you have the following beans initialized:
        - MetricRecorderFactory - an object implementing the MetricRecorderFactory interface.
        - AwsCredentialsProvider - an AwsCredentialsProvider object with your credentials in it.
        - config - a FluxCapacitorConfig object containing the desired configuration.

        This also assumes your Spring context contains a list of objects implementing the Workflow interface,
        which will be autowired into the FluxSpringInitializer bean.
    -->
    <bean id="fluxFactory" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringCreator" factory-method="createWithConfig">
        <constructor-arg ref="MetricRecorderFactory" />
        <constructor-arg ref="AwsCredentialsProvider" />
        <constructor-arg ref="config" />
    </bean>
    <bean id="fluxCapacitor" factory-bean="fluxFactory" factory-method="create" />
    <bean id="fluxInitializer" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringInitializer" />
</beans>
```

Similarly, you can use this config for your unit tests' spring context (you'll need a dependency on flux-testutils):

```xml
<beans>
    <bean id="fluxCapacitor" class="com.danielgmyers.flux.testutil.StubFluxCapacitor" />
    <bean id="fluxInitializer" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringInitializer" />
</beans>
```

You can then autowire in the StubFluxCapacitor object from your test's spring context.
