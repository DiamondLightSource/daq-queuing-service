# Queue Items

The items inside the queue contain either an `Experiment` object or a `TaskRequest` object. These determine which bluesky plans will run. Experiment objects match the format of an experiment in ulims, and can involve multiple bluesky plans. It always contains a sample, experiment definition and instrument session. A TaskRequest object matches the format blueapi expects to receive to run a single plan.

Allowing both in the queue means you can queue up whole experiments as well as individual bluesky plans.

The queue will convert the entire list of experiments and task requests to a list of blueapi calls (the task request objects are already in the right format). See [converters](./converters.md) on how this is done.

A queue item's status is derived from the status of the underlying bluesky plans it has produced:

- `Queued` - None of its bluesky plans have started
- `In progress` - At least one of its bluesky plans have started
- `Complete` - All of its bluesky plans have completed successfully
- `Error` - One of its bluesky plans failed
- `Cancelled` - The item was cancelled (removed from the queue)
