Migrating from 2.0.x to 2.1.0
====

A fairly large number of things have changed between 2.0.x and 2.1.0; this guide aims to streamline the work needed for migration.

Dependency changes
----

* The minimum required Java runtime is now ***Java 11***.
* `flux-swf` now has a dependency on Apache Commons-Codec.
* `flux-swf-guice` now depends on Guice 5.1 (was 4.2).
* Flux now depends on slf4j 2.0 (was 1.7).

Maven packaging changes
----

If you're migrating from 2.0.8 or later, the maven group IDs and package names have changed as follows:

* `com.danielgmyers.flux.clients.swf:flux-common` -> `com.danielgmyers.flux:flux-common`
* `com.danielgmyers.flux.clients.swf:flux-testutils` -> `com.danielgmyers.flux:flux-testutils`
* `com.danielgmyers.flux.clients.swf:flux` -> `com.danielgmyers.flux.clients.swf:flux-swf`
* `com.danielgmyers.flux.clients.swf:flux-guice` -> `com.danielgmyers.flux.clients.swf:flux-swf-guice`
* `com.danielgmyers.flux.clients.swf:flux-spring` -> `com.danielgmyers.flux.clients.swf:flux-swf-spring`

If you're migrating from 2.0.6 or earlier, the maven group IDs and package names have changed as follows:

* `software.amazon.aws.clients.swf.flux:flux-common` -> `com.danielgmyers.flux:flux-common`
* `software.amazon.aws.clients.swf.flux:flux-testutils` -> `com.danielgmyers.flux:flux-testutils`
* `software.amazon.aws.clients.swf.flux:flux` -> `com.danielgmyers.flux.clients.swf:flux-swf`
* `software.amazon.aws.clients.swf.flux:flux-guice` -> `com.danielgmyers.flux.clients.swf:flux-swf-guice`
* `software.amazon.aws.clients.swf.flux:flux-spring` -> `com.danielgmyers.flux.clients.swf:flux-swf-spring`

Version 2.0.7 was made available under both original maven group IDs.

Interface changes
----

The base java package for `FluxCapacitor`, `Workflow`, `WorkflowStep`, and other generic interfaces, annotations, and exceptions has been changed, and any that weren't in the `flux-common` maven package have been moved there:

* `software.amazon.aws.clients.swf.flux.FluxCapacitor` -> `com.danielgmyers.flux.FluxCapacitor`
* `software.amazon.aws.clients.swf.flux.RemoteWorkflowExecutor` -> `com.danielgmyers.flux.RemoteWorkflowExecutor`
* `software.amazon.aws.clients.swf.flux.WorkflowExecutionException` -> `com.danielgmyers.flux.ex.WorkflowExecutionException`
* `software.amazon.aws.clients.swf.flux.WorkflowStatusChecker` -> `com.danielgmyers.flux.WorkflowStatusChecker`
* `software.amazon.aws.clients.swf.flux.poller.BadWorkflowStateException` -> `com.danielgmyers.flux.ex.BadWorkflowStateException`
* `software.amazon.aws.clients.swf.flux.poller.UnrecognizedTaskException` -> `com.danielgmyers.flux.ex.UnrecognizedTaskException`
* `software.amazon.aws.clients.swf.flux.step.Attribute` -> `com.danielgmyers.flux.step.Attribute`
* `software.amazon.aws.clients.swf.flux.step.PartitionedWorkflowStep` -> `com.danielgmyers.flux.step.PartitionedWorkflowStep`
* `software.amazon.aws.clients.swf.flux.step.PartitionIdGenerator` -> `com.danielgmyers.flux.step.PartitionIdGenerator`
* `software.amazon.aws.clients.swf.flux.step.PartitionIdGeneratorResult` -> `com.danielgmyers.flux.step.PartitionIdGeneratorResult`
* `software.amazon.aws.clients.swf.flux.step.StepApply` -> `com.danielgmyers.flux.step.StepApply`
* `software.amazon.aws.clients.swf.flux.step.StepAttributes` -> `com.danielgmyers.flux.step.StepAttributes`
* `software.amazon.aws.clients.swf.flux.step.StepHook` -> `com.danielgmyers.flux.step.StepHook`
* `software.amazon.aws.clients.swf.flux.step.StepResult` -> `com.danielgmyers.flux.step.StepResult`
* `software.amazon.aws.clients.swf.flux.step.WorkflowStep` -> `com.danielgmyers.flux.step.WorkflowStep`
* `software.amazon.aws.clients.swf.flux.step.WorkflowStepHook` -> `com.danielgmyers.flux.step.WorkflowStepHook`
* `software.amazon.aws.clients.swf.flux.wf.Periodic` -> `com.danielgmyers.flux.wf.Periodic`
* `software.amazon.aws.clients.swf.flux.wf.Workflow` -> `com.danielgmyers.flux.wf.Workflow`
* `software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraph` -> `com.danielgmyers.flux.wf.graph.WorkflowGraph`
* `software.amazon.aws.clients.swf.flux.wf.graph.WorkflowGraphBuilder` -> `com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder`

`MetricRecorder` changes
----

The classes formerly in the java package `software.amazon.aws.clients.swf.flux.metrics` are now found in the following maven packages:

* `com.danielgmyers.metrics:recorder-core` - Contains the `MetricRecorder` interface and the `NoopMetricRecorder` implementation.
* `com.danielgmyers.metrics:in-memory-recorder` - Contains the `InMemoryMetricRecorder` implementation.

Additionally, the classes have changed their base java package from `software.amazon.aws.clients.swf.flux.metrics` to `com.danielgmyers.metrics`.

If you have a custom implementation of `MetricRecorder` you should update it to depend on `recorder-core` rather than `flux-common`, and update your `import` statements.

If you currently have a test dependency on `flux-testutils` in order to use `InMemoryMetricRecorder`, you will need a test dependency on the new `in-memory-recorder` package.

Partitioned workflow step changes
----

The `@PartitionIdGenerator` method of partitioned workflow steps must now always return an object of type `PartitionIdGeneratorResult`; `List<String>` is no longer allowed. Migration is straightforward: change your `List<String>` to a `Set<String>` and have your method return the output of `PartitionIdGeneratorResult.create(Set<String>)`.

Some pre-2.0.6 backward-compatibility code was removed; as a result, in order to upgrade to 2.1.0, you must either guarantee that all running partitioned workflow steps were started with Flux version 2.0.6 or later, or that no workflows with partitioned steps will be started during the migration.

There is new configurable behavior for partition ID handling. Prior to 2.1.0, partition IDs are used as-is as part of the activity id of workflow step executions; this imposes content and length constraints on partition ids. Users can now set the `EnablePartitionIdHashing` flag in `FluxCapacitorConfig`. If enabled, the activity id will contain a _SHA-256 hash_ of the partition id, but Flux will remove the content constraint and relax the length constraint on the user-specified partition id. The original user-provided partition id can still be found in the `control` field of the `ScheduleActivityTaskDecisionAttributes` history event associated with that step execution. Any tools which send signals will need to use the SHA-256 hash of the partition id when constructing the activity id for the signal.

Flux will attempt to properly handle any partitioned workflow steps that are running while you enable this feature, however the safest upgrade method is to ensure that no partitioned workflow steps are in progress while enabling this feature.

The `EnablePartitionIdHashing` feature is disabled by default in 2.1.0.

Workflow Step attribute changes
----

Prior to 2.1.0, if a `@StepApply` method requested a `Map<String, String>` attribute as one of its inputs, and that attribute was not present in the workflow metadata, Flux would provide an empty, mutable `HashMap<String, String>`. However, to help users understand the difference between "previous step did not provide the map" and "previous step provided an empty map", Flux no longer automatically provides an empty map in this case.

Also prior to 2.1.0, Flux would automatically convert attribute values between `Instant` and `Date` depending on which was requested by the step, regardless of which of the two types was actually used as the attribute type when the attribute was added earlier in the workflow. While this will still work *in practice* for 2.1.0, Flux ***no longer guarantees*** this behavior.

This also means that the WorkflowGraphBuilder's built-in attribute availability check no longer ignores the type difference between `Instant` and `Date` when evaluating attribute availability, including for the built-in `StepAttribute.ACTIVITY_INITIAL_ATTEMPT_TIME` and `StepAttribute.WORKFLOW_START_TIME` attributes (which are provided as `Instant`s). As a result, your `WorkflowGraphBuilder.build()` calls may being failing if you are presently relying on this behavior.

You should update your workflow input and output attributes to use `Instant` instead of `Date` in all cases.

Flux will drop support for the `Date` type in a future release.

Execution context changes
----

As discussed in [SIGNALS.md](./SIGNALS.md), Flux populates the workflow's execution context (accessible via the `latestExecutionContext` field in the response to the SWF `DescribeWorkflowExecution` API) with metadata intended to assist in using the `ForceResult` signal safely.

Prior to 2.1.0, the execution context contained a JSON-formatted map with two string values, both of which were themselves json values encoded as strings. For example:

```json
{
  "_nextStepName": "\"EatSandwich\"",
  "_nextStepSupportedResultCodes": "{\"_succeed\":\"DrinkWater\",\"_fail\":\"ThrowAwayBadSandwich\"}"
}
```

As of 2.1.0, the format of the execution context has been simplified. A version number field has been added to allow you to verify that you are parsing the context correctly, and the values of the other fields are no longer json-encoded strings. The context above would now look like this:

```json
{
  "fluxMetadataVersion": 2,
  "_nextStepName": "EatSandwich",
  "_nextStepSupportedResultCodes": {
    "_succeed": "DrinkWater",
    "_fail": "ThrowAwayBadSandwich"
  }
}
```

If you currently have tools that parse the execution context, you will need to update them to support both formats during the 2.1.0 upgrade, and then remove the old parsing logic once the upgrade is complete.

Input validation changes
----

Flux now directly imposes the following length limits on identifiers:

* Domain name: 256
* Workflow class implementation name: 127
* WorkflowStep class implementation name: 127
* Hostname: 154
* Task list name: 76
* Workflow execution ID: 256
* Workflow execution tag: 256
* Partition ID:
  * If `EnablePartitionIdHashing` = `false`: 123
  * If `EnablePartitionIdHashing` = `true`: 256

Additionally, because SWF imposes a character set constraint on certain identifiers, Flux will now enforce the following constraint (quoted from SWF's documentation):

```
The string must not contain a : (colon), / (slash), | (vertical bar), or any control characters (\u0000-\u001f | \u007f-\u009f). Also, it must not be the literal string "arn".
```

This constraint will be enforced on the following identifiers:

* Domain name
* Workflow class implementation name
* WorkflowStep class implementation name
* Task list name
* Workflow execution ID
* Partition ID
  * Only if `EnablePartitionIdHashing` = `false`; the literal string "arn" is still allowed.

RemoteWorkflowExecutor changes
----

If you use the RemoteWorkflowExecutor functionality, you'll need to provide a configuration callback to `FluxCapacitorConfig` for configuring the remote endpoint and credentials, rather than passing them directly to `FluxCapacitor.getRemoteWorkflowExecutor()`.

WorkflowStatusChecker changes
----

The method `WorkflowStatusChecker.getSwfClient()` was removed; this shouldn't have been needed outside of flux's internal tests, but if you found it useful please file an issue so we can add any missing functionality.

Deprecated methods removed
----

The following deprecated methods have been removed:

* `StepResult.success(String resultCode, String message)` - Use `StepResult.complete(String resultCode, String message)` instead.
* `StepResult.failure(String resultCode, String message)` - Use `StepResult.complete(String resultCode, String message)` instead.
* `FluxCapacitorConfig.getTaskListWorkerThreadCount()` - Use `FluxCapacitorConfig.getTaskListConfig()` instead.
* `FluxCapacitorConfig.setTaskListWorkerThreadCount()` - Use `FluxCapacitorConfig.putTaskListConfig()` instead.
* `FluxCapacitorConfig.getTaskListActivityTaskPollerThreadCount()` - Use `FluxCapacitorConfig.getTaskListConfig()` instead.
* `FluxCapacitorConfig.setTaskListActivityTaskPollerThreadCount()` - Use `FluxCapacitorConfig.putTaskListConfig()` instead.
* `FluxCapacitorConfig.getTaskListDeciderThreadCount()` - Use `FluxCapacitorConfig.getTaskListConfig()` instead.
* `FluxCapacitorConfig.setTaskListDeciderThreadCount()` - Use `FluxCapacitorConfig.putTaskListConfig()` instead.
* `FluxCapacitorConfig.getTaskListDecisionTaskPollerThreadCount()` - Use `FluxCapacitorConfig.getTaskListConfig()` instead.
* `FluxCapacitorConfig.setTaskListDecisionTaskPollerThreadCount()` - Use `FluxCapacitorConfig.putTaskListConfig()` instead.
* `FluxCapacitorConfig.getTaskListPeriodicSubmitterThreadCount()` - Use `FluxCapacitorConfig.getTaskListConfig()` instead.
* `FluxCapacitorConfig.setTaskListPeriodicSubmitterThreadCount()` - Use `FluxCapacitorConfig.putTaskListConfig()` instead.