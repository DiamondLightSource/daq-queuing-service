# Converters

Converters are plugins that can be used to do automatic manipulation and conversion of the queue. They can be used for:
- Validating new tasks added to the queue through the REST endpoint.
- Automatic pre-processing of the queue tasks, for example to add calibration scans before an experiment that requires them.
- Converting queue tasks (which may be in a ulims `Experiment` format) into executable blueapi calls.

By default, no validation, pre-processing or conversion will take place, and this is appropriate if the queue is being used simply as a queue of bluesky plans. However, the `Converter` class can be sub-classed, and it's methods overridden, to provide custom queue behaviour. There are three methods that the queue calls at different times:

```python
from daq_queuing_service.plugins.converter import Converter

class MyConverter(Converter):
    def validate(self, experiments: list[TaskRequest | Experiment]):
        ...
```

This is called whenever new tasks are added to the queue through the REST endpoint. If any errors occur here, the tasks will not be added to the queue and the error message is returned in the response.

```python
    def pre_process(
        self,
        queue: list[Task],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[Task]:
        ...
```

This is called whenever anything changes in the queue (e.g a task is added, or a running task finishes). It is an opportunity to manipulate the tasks in the queue based on its current contents and history. For example, this function could check if any tasks require a calibration scan to take place, and add it as a new task to the front of the queue, before returning the updated queue.

```python
def construct_blueapi_calls(
        self,
        queue: list[TaskWithPosition],
        history: list[TaskWithPosition],
        call_history: list[BlueapiCall],
    ) -> list[BlueapiCall]:
        ...
```

The queue contains ulims experiments and/or TaskRequests. While TaskRequests are already in the right format for blueapi to receive them, experiments need to be converted to blueapi calls for anything to run. This method is used for this conversion, constructing a list of BlueapiCall objects (which wrap TaskRequest objects) from the current queue contents. This is run whenever the queue changes, but after `pre_process`. The default converter assumes that the queue contains exclusively TaskRequests, and will raise a `NotImplementedError` if an `Experiment` is added. If you want to add ulims experiments to the queue, make sure you are using a custom converter that overrides this method!

Pre-processing and conversion is run any time the queue changes and right before a call is made to blueapi, so the blueapi call list stays in sync. This means these methods can make use of current information on a beamline such as PV values to inform their logic.

In config, under the converter section, add the path and name of the converter you want to use. For example, default config is:

```yaml
converter:
  path: "daq_queuing_service.plugins.converter"
  name: "Converter"
```
