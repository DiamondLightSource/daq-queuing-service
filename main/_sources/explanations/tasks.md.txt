Tasks
=====

The items contained inside the queue are called tasks. Tasks are produced by sending an experiment definition to the queue, which will validate it, assign it an ID it and create a task for it. Throughout the lifecycle of a task, information will be added to the task by the queue when it becomes available. This will include things like:
- Status of the task (Waiting, In Progress, Success etc)
- Time started
- Time completed
- Result of the task
- Any errors that occurred during the task
- A blueapi task ID (distinct from the ID assigned by the queue)

Once a task reaches the top of the queue, the experiment definition it contains will be converted to a blueapi call and sent to blueapi. Converter plugins can be written and interchanged, allowing for flexibility in how experiment definitions are written by different beamlines. Once a task is complete, it will leave the queue and enter a history list.
