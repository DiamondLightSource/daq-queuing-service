from blueapi.client import BlueapiClient
from blueapi.client.event_bus import EventBusClient
from blueapi.client.rest import BlueapiRestClient
from blueapi.config import ApplicationConfig
from bluesky_stomp.messaging import Broker, StompClient

from daq_queuing_service.blueapi_interaction.token_retriever import UDCTokenRetriever


def get_blueapi_client(blueapi_config: ApplicationConfig) -> BlueapiClient:
    blueapi_rest_client = BlueapiRestClient(
        config=blueapi_config.api,
        #  Waiting on https://github.com/DiamondLightSource/blueapi/pull/1553
        session_manager=UDCTokenRetriever(),  # type: ignore
    )

    if blueapi_config.stomp.enabled:
        assert blueapi_config.stomp.url.host is not None, "Stomp URL missing host"
        assert blueapi_config.stomp.url.port is not None, "Stomp URL missing port"
        stomp_client = StompClient.for_broker(
            broker=Broker(
                host=blueapi_config.stomp.url.host,
                port=blueapi_config.stomp.url.port,
                auth=blueapi_config.stomp.auth,
            )
        )
        events = EventBusClient(stomp_client)
        blueapi_client = BlueapiClient(blueapi_rest_client, events)
    else:
        blueapi_client = BlueapiClient(blueapi_rest_client)

    return blueapi_client
