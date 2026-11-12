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

# Queue

Items can be added, deleted, and moved around the queue through the REST API. The worker takes bluesky plans from the top of the queue, if one is available, and sends it to blueapi. Once this plan is in progress, it can't be moved or deleted, and no other plan can be sent to blueapi until this one has finished.

The queue can be paused, meaning no plan can be taken and sent to blueapi until the queue is unpaused. This will not pause a plan that is already in progress. While the queue is paused, tasks can still be added, deleted and moved around.

If an error occurs while running a plan, the queue will be paused, and any remaining plans that need to be run as part of the current experiment will be skipped.

Once an item is complete, it is added to a history list, which is a chronological list of completed tasks.

See the [API reference](../reference/rest-api.rst) for how to interact with the queue.
