Flux is a Java client library for SWF. This document provides basic code samples for getting started with Flux.

Flux is in production use by multiple public AWS services.

[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

[![CodeBuild status badge](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiSlhGMTFDV3dRRXJCdUxSYzdLQ3R0QzZ2WjArSWN2ZkZtVFhUbzB3L2FUZnJBZDhMbzFXQzI1WkpRRWUzdkgvcko4MFJoc1RHRW5BVmJZc3dMeWRQKzlvPSIsIml2UGFyYW1ldGVyU3BlYyI6IjcxN0ZlaXI0M3ZPdXVabDciLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)](https://us-west-2.codebuild.aws.amazon.com/project/eyJlbmNyeXB0ZWREYXRhIjoiVWZneFloWkVYSzZJc0wvRzVGeU1CQnc3a29ML0RIRmxUa29Hb1I5QXIvRTQ5T2JQbENHOVF3Sy93clI4WVYrM285Q3ExMVdzeUpXdmdqYW14czM0dDhtb2RrOXhmejIyelUzNCIsIml2UGFyYW1ldGVyU3BlYyI6Im5BcS85b3UxdkpzQWlyNkIiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D)

Flux quick start guide
======================

This quick start guide will walk you through writing a basic two-step "hello world" workflow.

Prerequisites
-------------

Flux uses the AWS SDK for Java v2.

Flux uses a custom `MetricRecorder` interface to emit workflow metrics; if you do not want Flux to emit metrics, you may provide Flux with a `com.danielgmyers.flux.clients.swf.metrics.NoopMetricRecorderFactory` object. If you want a different metrics implementation, you will need to provide an alternate implementation of the `com.danielgmyers.flux.clients.swf.metrics.MetricRecorder` interface.

Writing a basic workflow
------------------------

We'll start by writing a pair of workflow steps. First up is Hello:

```java
package example.flux;

import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;

public class Hello implements WorkflowStep {
    
    @StepApply
    public void greetings() {
        System.out.println("Hello!");
    }
}
```

All workflow steps must implement the `WorkflowStep` interface. There are no methods that you are required to override; however, you must implement exactly *one* method that has the `@StepApply` annotation. This is the method Flux will execute when your workflow reaches this step.

The return type of your `@StepApply` method may be of any type, or `void`; however, Flux implements special result handling logic if the return type is `com.danielgmyers.flux.clients.swf.step.StepResult`. This is the mechanism you use if you want to include additional attributes in your workflow metadata (for use by later steps) or if you want to return with a custom result code (e.g. for creating branches in your workflow logic).

If the `@StepApply` method returns `StepResult.success()` or otherwise returns successfully, Flux will consider the workflow step to be completed. If the method throws an exception, Flux will schedule the step to be retried.

Now, let's implement the second step, `Goodbye`:

```java
package example.flux;

import com.danielgmyers.flux.clients.swf.step.Attribute;
import com.danielgmyers.flux.clients.swf.step.StepApply;
import com.danielgmyers.flux.clients.swf.step.StepResult;
import com.danielgmyers.flux.clients.swf.step.WorkflowStep;

public class Goodbye implements WorkflowStep {
    
    @StepApply
    public StepResult greetings(@Attribute("friend") String friend) {
        if (friend == null) {
            System.out.println("Goodbye!");
        } else {
            System.out.println("Goodbye, " + friend + "!");
        }
        return StepResult.success("I managed to say goodbye.");
    }
}
```

This step is set up the same as `Hello`, except that its `@StepApply` method has an extra parameter. Workflow steps may request named input parameters; Flux will attempt to find a matching entry in the workflow attributes map for that workflow execution and, if found, pass it in to the step. If no matching attribute is found, Flux will pass in `null`.

Flux supports `@Attribute` parameters of any of the following types:
- `String`
- `Long`
- `Date`
- `Instant`
- `Boolean`
- `Map<String, String>`
- `com.danielgmyers.flux.clients.swf.metrics.MetricRecorder`

If more complex types are needed, it is recommended that you serialize the value into a `String` or a `Map<String, String>`.

Note that both workflow steps are implemented in a self-contained manner; workflow steps should document their behavior and input/output contracts thoroughly, without considering which specific workflow they maybe added to (provided the required inputs are present). Additionally, workflow step implementations should be threadsafe and, ideally, idempotent. Following these guidelines will result in workflow steps that are easily testable and easily reusable across multiple workflows.

Finally, we need to create the workflow itself:

```java
package example.flux;

import com.danielgmyers.flux.clients.swf.wf.Workflow;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph;
import com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder;

// If you're using Spring, you should consider annotating this class with @Component for convenience.
public class HelloGoodbye implements Workflow {
    
    private WorkflowGraph graph;
    
    public HelloGoodbye() {
        // First, we initialize objects for each step, they will be reused for all workflow executions.
        WorkflowStep hello = new Hello();
        WorkflowStep goodbye = new Goodbye();
        
        // Next we create the WorkflowGraphBuilder, a helper class for defining your workflow's structure.
        // WorkflowGraphBuilder requires the first step of the workflow to be passed in to its constructor.
        // Afterward, at least one transition should be defined for the initial step.
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(hello);
        builder.alwaysTransition(hello, goodbye);
        
        // Next we add the second step of the workflow to the graph...
        builder.addStep(goodbye);
        // ... and define a transition that always closes the workflow when the step completes.
        builder.alwaysClose(goodbye);
        
        // finally, we build the graph and store it for later use.
        this.graph = builder.build();
    }
    
    /**
     * All Workflow objects must implement this method.
     * 
     * Flux will call this method repeatedly at runtime, so it is strongly recommended that Workflow objects
     * be singletons, and that the graph be constructed exactly once in the constructor.
     */
    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
```

There are comments above explaining what each of those sections of code do; the important thing to note is that there is no actual business logic in this class, its sole job is to define the structure of the workflow.

`WorkflowGraphBuilder` is where the guts of Flux's development-time power lives. When you call `build()` on the builder, Flux validates that the graph you have specified meets certain criteria:

- The workflow has at least one step (it is passed to the constructor of the `WorkflowGraphBuilder`).
- All steps must be reachable.
- At least one transition is defined for each step (either to another step, or to close the workflow).
- The graph may not contain loops. (Users with loop-like use cases should explore using multiple runs of the same workflow, or partitioned workflow steps, instead.)
- Each step is added only once, and the workflow step classes do not have any conflicting simple names.
- Each step defines exactly one `@StepApply` method, and each of its parameters are of the allowed types and have the correct annotation.

Because the workflow graph is constructed at startup (including in your unit tests), you can be confident that your graph meets the above-mentioned criteria before you deploy the code or run your workflow for the first time. This graph validation serves as unit-test-time validation of the overall workflow; individual workflow step classes should be tested individually and independently.

`WorkflowGraphBuilder` can do additional validation on your workflow definition; see the wiki for more information.

Workflow branches
---------------------------------------
It is often useful to be able to take different paths through a workflow depending on the outcome of a step. For example, a workflow step may determine that a required action will be impossible, and the workflow should proceed to a series of rollback steps.

To support this kind of use case, Flux offers the capability to define the path a workflow will take through its steps based on the "result code" returned by each step. Flux offers two default result codes (`StepResult.SUCCEED_RESULT_CODE` and `StepResult.FAIL_RESULT_CODE`) that meet most needs, and supports arbitrary custom result codes to support more complex use cases.

In this example, we create a workflow with three main steps, and rollback steps which are the inverse of the main three steps. We will assume those step classes are already defined.

```java
public class ExampleBranchingWorkflow implements Workflow {

    private WorkflowGraph graph;

    public ExampleBranchingWorkflow() {
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(new StepOne());
        builder.successTransition(StepOne.class, StepTwo.class);
        builder.failureTransition(StepOne.class, RollbackStepOne.class);

        // The commonTransitions() helper is equivalent to the separate successTransition() and failureTransition() calls above.
        builder.addStep(new StepTwo());
        builder.commonTransitions(StepTwo.class, StepThree.class, RollbackStepTwo.class);

        builder.addStep(new StepThree());
        builder.closeOnSuccess(StepThree.class);
        builder.failureTransition(StepThree.class, RollbackStepThree.class);

        // Now we define the rollback branch
        // Since this is just like any branch, it's best to define them in the order they execute in.
        builder.addStep(new RollbackStepThree());
        // In this case, we always want to go to the next rollback step even if RollbackStepThree returns a failure result.
        builder.commonTransitions(RollbackStepThree.class, RollbackStepTwo.class, RollbackStepTwo.class);

        // alwaysTransition() can be used instead of commonTransitions() when the success and failure transitions are to the same step.
        builder.addStep(new RollbackStepTwo());
        builder.alwaysTransition(RollbackStepTwo.class, RollbackStepOne.class);

        builder.addStep(new RollbackStepOne());
        builder.alwaysClose(RollbackStepOne.class);

        graph = builder.build();
    }

    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
```


Here we used the default success and failure result codes to define a simple branching workflow. `WorkflowGraphBuilder` offers helper methods like `commonTransitions` and `alwaysTransition` to make the most common configurations easier.

If instead we wanted to use custom result codes for all of these transitions, it could be done like this:

```java
public class ExampleBranchingWorkflow implements Workflow {

    private WorkflowGraph graph;

    public ExampleBranchingWorkflow() {
        WorkflowGraphBuilder builder = new WorkflowGraphBuilder(new StepOne());
        builder.customTransition(StepOne.class, "won", StepTwo.class);
        builder.customTransition(StepOne.class, "lost", RollbackStepOne.class);

        // The commonTransitions() helper is equivalent to the separate successTransition() and failureTransition() calls above.
        builder.addStep(new StepTwo());
        builder.customTransition(StepTwo.class, "retained", StepThree.class);
        builder.customTransition(StepTwo.class, "revoked", RollbackStepTwo.class);

        builder.addStep(new StepThree());
        builder.closeOnCustom(StepThree.class, "lived");
        builder.customTransition(StepThree.class, "died", RollbackStepThree.class);

        // Now we define the rollback branch.
        builder.addStep(new RollbackStepThree());
        builder.customTransition(RollbackStepThree.class, "resurrected", RollbackStepTwo.class);

        // alwaysTransition() works for custom result codes too; Flux ignores the actual result code returned by the step if you define an "always" transition.
        builder.addStep(new RollbackStepTwo());
        builder.alwaysTransition(RollbackStepTwo.class, RollbackStepOne.class);

        builder.addStep(new RollbackStepOne());
        builder.alwaysClose(RollbackStepOne.class);

        graph = builder.build();
    }

    @Override
    public WorkflowGraph getGraph() {
        return graph;
    }
}
```

For clarity, this is how a workflow step would actually return a result with a custom code:

```java
public class StepTwo implements WorkflowStep {
    @StepApply
    public StepResult decideSomething(@Attribute("someInput") String value) {
        if ("diamond".equals(value)) {
            return StepResult.complete("retained", "We decided to keep the value since it's a diamond.");
        } else {
            return StepResult.complete("revoked", "We decided not to keep the value.");
        }
    }
}
```

Library initialization
---------------------------------------

```java
package example.flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.danielgmyers.flux.clients.swf.FluxCapacitor;
import com.danielgmyers.flux.clients.swf.FluxCapacitorConfig;
import com.danielgmyers.flux.clients.swf.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.swf.metrics.MetricRecorderFactory;
import com.danielgmyers.flux.clients.swf.metrics.NoopMetricRecorderFactory;
import com.danielgmyers.flux.clients.swf.wf.Workflow;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class MyApp {

    public static void main() {
        List<Workflow> workflows = new ArrayList<>();
        workflows.add(new HelloGoodbye());

        FluxCapacitorConfig config = new FluxCapacitorConfig();
        config.setAwsRegion("us-west-2"); // optional, the SDK will determine the region from your environment if possible
        config.setSwfDomain("myapp"); // SWF uses this to namespace your workflows in your account

        // You can use any AwsCredentialsProvider, provided the credentials have swf:* permissions.
        AwsCredentialsProvider creds = new DefaultCredentialsProvider();

        MetricRecorderFactory metricsFactory = new NoopMetricRecorderFactory();
        
        FluxCapacitor fluxCapacitor = FluxCapacitorFactory.create(metricsFactory, creds, config);
        fluxCapacitor.initialize(workflows);
        
        // All done! Start a workflow like so:
        fluxCapacitor.executeWorkflow(HelloGoodbye.class, "test-workflow-foo-bar", Collections.emptyMap());

        // If you'd like a clean shutdown, you can call these methods:
        fluxCapacitor.shutdown();
        fluxCapacitor.awaitTermination(60, TimeUnit.SECONDS);
    }

}
```

Unit testing workflow steps
---------------------------

In the `flux-testutils` package, Flux provides a utility class `StepValidator` that should be used to validate input to your workflows. It is strongly recommended that you use `StepValidator` to test your steps, instead of calling your step's `@StepApply` method directly, because `StepValidator` uses the same `@StepApply` execution logic that Flux uses at runtime (including converting thrown exceptions into "retry" results).

```java
package example.flux;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.step.WorkflowStep;
import com.danielgmyers.flux.clients.swf.testutil.StepValidator;

import org.junit.Test;

public class HelloTest {

    @Test
    public void testHello() {
        WorkflowStep hello = new Hello();
        
        // first construct whatever input you want to test your step with
        Map<String, Object> input = new HashMap<>();
        input.put("name", "John");
        input.put("age", 42L);
        input.put("isHuman", true);
        input.put("currentTime", new Date());

        // this method will throw a junit assertion exception if the step's result does not match the expected result.
        StepValidator.succeeds(hello, input);
    }
}
```

`StepValidator` supports the following helper methods (among others), each of which accepts a `WorkflowStep` object and a map of input parameters:
- `succeeds` - Asserts that the step returns successfully, either by returning `StepResult.success()` or by successfully returning with any other type (including `void`).
- `fails` - Asserts that the step returns unsuccessfully, by returning `StepResult.failure()`.
- `retries` - Asserts that the step would be scheduled for a retry, either because it threw an exception or because it returned `StepResult.retry()`.
- `completes` - Asserts that the step completed (i.e. will not retry) with a specific result code.

Additionally, `flux-testutils` provides `InMemoryMetricRecorder` that stores metrics in memory so that you can validate that your step logic emits the right metrics.

There are also `StubFluxCapacitor` and `StubRemoteWorkflowExecutor` implementations for unit testing code that takes a `FluxCapacitor` or `RemoteWorkflowExecutor` as input, for example:

```java
package example.flux;

import java.util.HashMap;
import java.util.Map;

import com.danielgmyers.flux.clients.swf.FluxCapacitorFactory;
import com.danielgmyers.flux.clients.swf.testutil.StubFluxCapacitor;

import org.junit.Assert;
import org.junit.Test;

public class MyAppTest {
    
    @Test
    public void testSomething() {
        StubFluxCapacitor stubFluxCapacitor = FluxCapacitorFactory.createMock();

        // call some code that initiates a workflow

        stubFluxCapacitor.verifyWorkflowWasNotStarted(WorkflowThatShouldNotRun.class, "some-id");
        
        Map<String, String> expectedInput = new HashMap<>();
        stubFluxCapacitor.verifyWorkflowWasStarted(WorkflowThatShouldRun.class, "some-id", expectedInput);
        
        Assert.assertEquals(1, stubFluxCapacitor.countExecutedWorkflows());
        
        // you can use this method if you're sharing your stubFluxCapacitor object across tests:
        stubFluxCapacitor.resetExecutionCache();
    }
}
```