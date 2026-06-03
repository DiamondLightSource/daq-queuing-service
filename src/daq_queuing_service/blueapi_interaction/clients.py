from unittest.mock import MagicMock

from blueapi.client import BlueapiClient
from blueapi.client.event_bus import EventBusClient
from blueapi.client.rest import BlueapiRestClient
from blueapi.config import ApplicationConfig
from blueapi.service.authentication import SessionCacheManager
from bluesky_stomp.messaging import Broker, StompClient

from daq_queuing_service.blueapi_interaction.session_manager import UDCSessionManager


def get_blueapi_clients(
    blueapi_config: ApplicationConfig,
) -> tuple[BlueapiRestClient, BlueapiClient]:
    # This should be able to be simplified once the blueapi client supports UDC.
    if not blueapi_config.oidc:
        blueapi_config.oidc = MagicMock()

    session_manager = UDCSessionManager(blueapi_config.oidc, SessionCacheManager(None))
    blueapi_rest_client = BlueapiRestClient(
        config=blueapi_config.api, session_manager=session_manager
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

    return blueapi_rest_client, blueapi_client
