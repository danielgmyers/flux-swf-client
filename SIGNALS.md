
Flux supports using SWF's workflow signaling mechanism to affect the behavior of a running workflow in a few specific ways.

To send a signal, the `SignalName` field in the SWF `SignalWorkflowExecution` request should be the name of the signal exactly as documented below, and the input to the signal should be a map of input attributes, in json format, containing the attributes required for that signal. Since these signals are typically sent from a UI (such as an operational console or the AWS Management Console), or via CLI commands, and since these signals are not normally useful except for extraordinary operational needs, the FluxCapacitor interface does not provide a way to send these signals from within your application.

All signals require, as one of their inputs, a field named `activityId`, which should match the activity id of the _next_ execution of the step. This will match the id of the open retry timer. For example, if a step is named `EatSandwich`, and the fifth retry of the step is scheduled, there will be an open timer with the id `EatSandwich_5`, and when that step actually executes again, that activity's id will be `EatSandwich_5`.

Delay Retry
-----------

This signal tells Flux to cancel the current retry timer and reschedule it with the specified delay (in seconds). The new timer will be set to exactly the delay time provided, with no jitter or other modifications applied to it.

Signal name: `DelayRetry`

Example input:
```json
{
  "activityId": "EatSandwich_5",
  "delayInSeconds": 543
}
```

Retry Now
---------

This signal tells Flux to cancel the current retry timer and reschedule the step immediately. This is functionally equivalent to sending the DelayRetry signal with a delay of 0 seconds.

Signal name: `RetryNow`

Example input:
```json
{
  "activityId": "EatSandwich_5"
}
```

Force Result
------------

This signal tells flux to cancel the current retry timer and pretend the step completed with the specified result code. Note that if your graph does not define a transition for that result code, your workflow will get stuck; to fix this, send another ForceResult signal with the same activityId containing a result code that your graph _does_ define a transition for.

The two default result codes are `_succeed` and `_fail`, but you may specify any result code defined by your workflow graph (except partitioned steps, which only support the two default codes).

This is most useful for forcing a workflow into a rollback path.

Signal name: `ForceResult`

Example input:
```json
{
  "activityId": "EatSandwich_5",
  "resultCode": "_fail"
}
```

Note that if you send this signal to a partitioned step, the result code applies only to that specific partition; also note that partitioned steps only support the two default result codes.

To aid in building tools that can send the Force Result signal, Flux populates the workflow's execution context (accessible via the `latestExecutionContext` field in the response to the SWF `DescribeWorkflowExecution` API) with the list of step result codes that the workflow graph defines for the current step, as well as information about which step each of those result codes will cause the workflow to transition to.

The execution context will be a json object as follows:

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

The `fluxMetadataVersion` field is provided by Flux to help you know how to parse the execution context. This will typically only be useful while upgrading Flux when the metadata format has changed, but you should check this value in your tooling to be sure you notice if the format changes.

`_nextStepName` indicates which step the workflow will run next (keeping in mind that this value is set at the same time as the next step is scheduled).

For each result code supported by that step, `_nextStepSupportedResultCodes` indicates which workflow step will be executed after this one if it completes with that result code.

Note that if the result code map contains a mapping for the special `_always` result code, then the indicated step will always be executed, regardless of the actual result code returned by the step or specified in a signal. 