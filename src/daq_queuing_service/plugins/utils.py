import importlib

from daq_queuing_service.plugins.base_plugin import QueuePlugin


class PluginError(Exception):
    def __init__(self, original: Exception):
        super().__init__(f"{type(original).__name__}: {original}")
        self.original = original


class ValidateError(Exception): ...


def get_queue_plugin(path: str, name: str) -> QueuePlugin:
    """Instantiates a queue plugin based on a path and class name

    Args:
        path (str): Path to plugin class. For example:
            "daq_queuing_service.plugins"
        name (str): Name of the plugin class. For example:
            "QueuePlugin"

    Returns:
        QueuePlugin: QueuePlugin instance
    """
    module = importlib.import_module(path)
    plugin_cls = getattr(module, name)
    plugin = plugin_cls()
    if not isinstance(plugin, QueuePlugin):
        raise TypeError(f"Plugin is not of type QueuePlugin, it is {type(plugin)}")
    return plugin
