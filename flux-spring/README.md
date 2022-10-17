
Library initialization with Spring
------------------------------------

You'll need a dependency on `flux-spring` to make use of this example config.

If you're using Spring, it is recommended to initialize your Workflow objects as singleton beans, either via component-scan or xml configuration.

```xml
<beans>
    <!-- This assumes you have the following beans initialized:
        - MetricRecorderFactory - an object implementing the MetricRecorderFactory interface.
        - AWSCredentials - an AWSCredentialsProvider object with your credentials in it.
        - swfRegion - a String containing the swf region you want to use.
        - swfEndpoint - a String containing the swf endpoint Flux should connect to.
        - workflowDomain - a String containing the workflow domain Flux should use to register and execute workflows.
        
        This also assumes your Spring context contains a list of objects implementing the Workflow interface,
        which will be autowired into the FluxSpringInitializer bean.
    -->

    <bean id="fluxFactory" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringCreator" factory-method="createWithConfig">
        <constructor-arg ref="MetricRecorderFactory" />
        <constructor-arg ref="AWSCredentials" />
        <constructor-arg ref="swfRegion" />
        <constructor-arg ref="swfEndpoint" />
        <constructor-arg ref="workflowDomain" />
    </bean>
    <bean id="fluxCapacitor" factory-bean="fluxFactory" factory-method="create" />
    <bean id="fluxInitializer" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringInitializer" />
</beans>
```

Similarly, you can use this config for your unit tests' spring context:

```xml
<beans>
    <bean id="fluxCapacitor" class="com.danielgmyers.flux.clients.swf.FluxCapacitorFactory" factory-method="createMock" />
    <bean id="fluxInitializer" class="com.danielgmyers.flux.clients.swf.spring.FluxSpringInitializer" />
</beans>
```

You can then autowire in a StubFluxCapacitor object from your test's spring context.
