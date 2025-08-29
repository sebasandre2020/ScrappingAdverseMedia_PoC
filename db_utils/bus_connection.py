import logging
import os
import json
import sys
from itertools import islice

sys.path.append(os.path.join(os.getcwd(), '.venv', 'Lib', 'site-packages'))

from azure.servicebus import ServiceBusClient, ServiceBusMessage

service_bus_client = None

def get_service_bus_client() -> ServiceBusClient:
    global service_bus_client

    if not service_bus_client:
        try:
            logging.info("Establishing a connection to Service Bus.")
            connection_string = os.environ.get("ServiceBusConnectionSetting")
            if not connection_string:
                logging.error("ServiceBusConnectionSetting environment variable is not set.")
                raise EnvironmentError("ServiceBusConnectionSetting environment variable is missing.")
            service_bus_client = ServiceBusClient.from_connection_string(connection_string)
        except Exception as e:
            logging.error(f"Failed to connect to Service Bus: {e}")
            raise Exception("Could not connect to Service Bus.")
    
    return service_bus_client

def enqueue_service_bus_messages(client: ServiceBusClient, queue_name: str, messages: list, session_id, batch_size: int = 1) -> None:
    if client is None:
        raise Exception("Service Bus client is not initialized.")
    
    try:
        sender = client.get_queue_sender(queue_name)
        with sender:
            for batch_start in range(0, len(messages), batch_size):
                batch_messages = messages[batch_start:batch_start + batch_size]
                
                aggregated_message = json.dumps({"messages": batch_messages})
                service_bus_message = ServiceBusMessage(aggregated_message, session_id=str(session_id))
                
                sender.send_messages(service_bus_message)
                # logging.warning(
                #     f"Successfully enqueued an aggregated message containing {len(batch_messages)} original messages to Service Bus queue '{queue_name}'."
                # )
                
        logging.warning(f"Successfully enqueued {len(messages)} messages to Service Bus queue '{queue_name}'.")

    except Exception as e:
        logging.error(f"Failed to enqueue messages to Service Bus: {e}")
        raise Exception("Error occurred while enqueuing messages.")