# Queue

Items can be added, deleted, and moved around the queue through the REST API. The worker takes bluesky plans from the top of the queue, if one is available, and sends it to blueapi. Once this plan is in progress, it can't be moved or deleted, and no other plan can be sent to blueapi until this one has finished. An experiment's status is `In progress` if any of its underlying plans have started. It is `Complete` once all of its plans have finished.

The queue can be paused, meaning no plan can be taken and sent to blueapi until the queue is unpaused. This will not pause a plan that is already in progress. While the queue is paused, tasks can still be added, deleted and moved around.

If an error occurs while running a plan, the queue will be paused, and any remaining plans that need to be run as part of the current experiment will be skipped.

Once an item is complete, it is added to a history list, which is a chronological list of completed tasks.

See the [API reference](../reference/rest-api.rst) for how to interact with the queue.
