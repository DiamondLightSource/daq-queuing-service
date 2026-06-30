# Queue Items

The items inside the queue contain either an `Experiment` object or a `TaskRequest` object. These determine which bluesky plans will run. Experiment objects match the format of an Experiment in ulims, and can involve multiple bluesky plans. It always contains a sample, experiment definition and instrument session. A TaskRequest object matches the format blueapi expects to receive to run a single plan.

Allowing both in the queue means you can queue up whole experiments as well as individual bluesky plans.

The queue will convert the entire list of Experiments and TaskRequests to a list of blueapi calls (the task request objects are already in the right format). See [converters](./converters.md) on how this is done.
