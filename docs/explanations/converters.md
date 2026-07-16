# Converters

The queue contains ulims experiments and/or TaskRequests. While TaskRequests are already in the right format for blueapi to receive them, experiments need to be converted to blueapi calls for anything to run. This conversion is beamline specific and configurable. The default converter simply passes the parameters of the experiment definition directly to blueapi with no conversion taking place.

Converters take the list of queued tasks, the queue history, and the blueapi call history (previous calls made to blueapi) and use this information to construct a list of future blueapi calls. Not all of this information needs to be used by the converter (for example, the default converter only uses the list of queued tasks to inform its 'conversion') but the interface of a converter function needs to be as follows:

```python
def my_converter(
    queue: list[TaskWithPosition],
    history: list[TaskWithPosition],
    call_history: list[BlueapiCall]
) -> list[BlueapiCall]:
    ...
```

Conversion is run any time the queue changes and right before a call is made to blueapi, so the blueapi call list stays in sync. This means converters can make use of current information on a beamline such as PV values to inform the conversion.

Note that since the queue can contain TaskRequest objects, a converter should be able to handle this. It should almost always pass them through with no conversion

In config, under the converter section, add the path and name of the converter you want to use. For example:

```yaml
converter:
  path: "daq_queuing_service.plugins.converter"
  name: "Converter"
```
