# Notify to slack

You can trigger slack notifications by formatting log messages a certain way. The [notify](https://diamondlightsource.github.io/daq-queuing-service/main/reference/log.html#daq_queuing_service.log.notify) helper function does this for you, so that there is a single, consistent way to trigger notifications. See [docs](https://gitlab.diamond.ac.uk/sscc-docs/developer-guide/-/merge_requests/295) on how this works. Note that an instrument should always be set, so that an event definition in graylog can filter notification logs and notify the relevant slack channel for a given log message. This can be done by calling `setup_logger` and providing an instrument.

See this [i15-1 event definition](https://graylog.diamond.ac.uk/alerts/definitions/6adb2e7cc6cc415af7f2c9fb), and set one up to pick up notifications for your instrument if one doesn't yet exist.

This [notification](https://graylog.diamond.ac.uk/alerts/notifications/6abbfaefc6cc415af78f167a) configures how logs picked up by the above event definition are formatted and where to send them.
