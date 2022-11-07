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

The maven group IDs and package names have changed as follows:

* `com.danielgmyers.flux.clients.swf:flux-swf-client-pom` -> `com.danielgmyers.flux:flux-base-pom`
* `com.danielgmyers.flux.clients.swf:flux-common` -> `com.danielgmyers.flux:flux-common`
* `com.danielgmyers.flux.clients.swf:flux-testutils` -> `com.danielgmyers.flux:flux-testutils`
* `com.danielgmyers.flux.clients.swf:flux` -> `com.danielgmyers.flux.clients.swf:flux-swf`
* `com.danielgmyers.flux.clients.swf:flux-guice` -> `com.danielgmyers.flux.clients.swf:flux-swf-guice`
* `com.danielgmyers.flux.clients.swf:flux-spring` -> `com.danielgmyers.flux.clients.swf:flux-swf-spring`

Interface changes
----

The base java package for the `FluxCapacitor`, `Workflow`, `WorkflowStep`, and other generic interfaces and annotations has been changed, and any that weren't in the `flux-common` maven package have been moved there:

* `com.danielgmyers.flux.clients.swf.FluxCapacitor` -> `com.danielgmyers.flux.FluxCapacitor`
* `com.danielgmyers.flux.clients.swf.RemoteWorkflowExecutor` -> `com.danielgmyers.flux.RemoteWorkflowExecutor`
* `com.danielgmyers.flux.clients.swf.WorkflowStatusChecker` -> `com.danielgmyers.flux.WorkflowStatusChecker`
* `com.danielgmyers.flux.clients.swf.step.Attribute` -> `com.danielgmyers.flux.step.Attribute`
* `com.danielgmyers.flux.clients.swf.step.PartitionedWorkflowStep` -> `com.danielgmyers.flux.step.PartitionedWorkflowStep`
* `com.danielgmyers.flux.clients.swf.step.PartitionIdGenerator` -> `com.danielgmyers.flux.step.PartitionIdGeneratorResult`
* `com.danielgmyers.flux.clients.swf.step.StepApply` -> `com.danielgmyers.flux.step.StepApply`
* `com.danielgmyers.flux.clients.swf.step.StepAttributes` -> `com.danielgmyers.flux.step.StepAttributes`
* `com.danielgmyers.flux.clients.swf.step.StepHook` -> `com.danielgmyers.flux.step.StepHook`
* `com.danielgmyers.flux.clients.swf.step.StepResult` -> `com.danielgmyers.flux.step.StepResult`
* `com.danielgmyers.flux.clients.swf.step.WorkflowStep` -> `com.danielgmyers.flux.step.WorkflowStep`
* `com.danielgmyers.flux.clients.swf.step.WorkflowStepHook` -> `com.danielgmyers.flux.step.WorkflowStepHook`
* `com.danielgmyers.flux.clients.swf.wf.Periodic` -> `com.danielgmyers.flux.wf.Periodic`
* `com.danielgmyers.flux.clients.swf.wf.Workflow` -> `com.danielgmyers.flux.wf.Workflow`
* `com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraph` -> `com.danielgmyers.flux.wf.graph.WorkflowGraph`
* `com.danielgmyers.flux.clients.swf.wf.graph.WorkflowGraphBuilder` -> `com.danielgmyers.flux.wf.graph.WorkflowGraphBuilder`

`MetricRecorder` changes
----

The classes formerly in the java package `com.danielgmyers.flux.clients.swf.metrics` are now found in the following maven packages:

* `com.danielgmyers.metrics:recorder-core` - Contains the `MetricRecorder` interface and the `NoopMetricRecorder` implementation.
* `com.danielgmyers.metrics:in-memory-recorder` - Contains the `InMemoryMetricRecorder` implementation.

Additionally, the classes have changed their base java package from `com.danielgmyers.flux.clients.swf.metrics` to `com.danielgmyers.metrics`.

If you have a custom implementation of `MetricRecorder` you should update it to depend on `recorder-core` rather than `flux-common`, and update your `import` statements.

If you currently have a test dependency on `flux-testutils` in order to use `InMemoryMetricRecorder`, you will need a test dependency on the new `in-memory-recorder` package.

Partitioned workflow step changes
----

The `@PartitionIdGenerator` method of partitioned workflow steps must now always return an object of type `PartitionIdGeneratorResult`; `List<String>` is no longer allowed. Migration is straightforward: change your `List<String>` to a `Set<String>` and have your method return the output of `PartitionIdGeneratorResult.create(Set<String>)`.

Some pre-2.0.6 backward-compatibility code was removed; as a result, in order to upgrade to 2.1.0, you must either guarantee that all running partitioned workflow steps were started with Flux version 2.0.6 or later, or that no workflows with partitioned steps will be started during the migration.

There is new configurable behavior for partition ID handling. Prior to 2.1.0, partition IDs are used as-is as part of the activity id of workflow step executions; this imposes content and length constraints on partition ids. Users can now set the `EnablePartitionIdHashing` flag in `FluxCapacitorConfig`. If enabled, the activity id will contain a _SHA-256 hash_ of the partition id, but Flux will remove the content constraint and relax the length constraint on the user-specified partition id. The original user-provided partition id can still be found in the `control` field of the `ScheduleActivityTaskDecisionAttributes` history event associated with that step execution. Any tools which send signals will need to use the SHA-256 hash of the partition id when constructing the activity id for the signal.

Flux will attempt to properly handle any partitioned workflow steps that are running while you enable this feature, however the safest upgrade method is to ensure that no partitioned workflow steps are in progress while enabling this feature.

The `EnablePartitionIdHashing` feature is disabled by default in 2.1.0.

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