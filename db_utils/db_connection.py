import logging
import os
from azure.cosmos import CosmosClient, exceptions, ContainerProxy

# Static global variable
cosmos_client = None

def get_cosmos_client() -> CosmosClient:
    global cosmos_client
    
    if not cosmos_client:
        try:
            logging.info('Establishing a connection to the database.')
            endpoint = os.environ.get("COSMOS_DB_URI")
            key = os.environ.get("COSMOS_DB_KEY")
            if not endpoint or not key:
                logging.error("COSMOS_DB_URI or COSMOS_DB_KEY environment variables are not set correctly.")
                raise EnvironmentError("COSMOS_DB_URI or COSMOS_DB_KEY environment variables are not set correctly.")
            cosmos_client = CosmosClient(endpoint, credential=key)
        except Exception as e:
            logging.error("COSMOS_DB_URI or COSMOS_DB_KEY environment variables are not set correctly.")
            raise Exception("Could not connect to Cosmos DB.")
    return cosmos_client

def cosmos_container_connection(client: CosmosClient, database_name: str, container_name: str) -> ContainerProxy:
    if client is None:
        raise Exception("Cosmos DB client is not initialized.")
    try:
        database = client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        return container
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f'Error connecting to Cosmos DB: {str(e)}')
        raise Exception("Could not connect to Cosmos DB in the stablished database and container.")
