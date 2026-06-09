Queue
=====

The queue is essentially a list of [tasks](./tasks.md). Tasks can be added, deleted, and moved around the queue through the REST API. A worker takes the task at the top of the queue, if one is available, and sends it to blueapi. Once this task is in progress, it can't be moved or deleted, and no other task can be sent to blueapi until this one has finished. 

The queue can be paused, meaning no task can be taken and sent to blueapi until the queue is unpaused. This will not pause a task that is already in progress. While the queue is paused, tasks can still be added, deleted and moved around.

Once a task is complete, it is added to a history list, which is a chronological list of completed tasks.

See the [API reference](../reference/rest-api.rst) for how to interact with the queue.
