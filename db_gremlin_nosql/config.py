"""
Centralized configuration file for connecting to Azure Cosmos DB
using the Gremlin (Graph) API.

This file loads environment variables and exposes
configuration constants used across the project.
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Path to .env outside the project folder
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)



# Azure Cosmos DB account name
ACCOUNT = os.getenv("COSMOS_ACCOUNT")

# Database name
DATABASE = os.getenv("COSMOS_DATABASE")

# Graph (collection) name
GRAPH = os.getenv("COSMOS_GRAPH")

# Primary access key (secret)
PRIMARY_KEY = os.getenv("COSMOS_PRIMARY_KEY")

# WebSocket endpoint for Gremlin API
ENDPOINT = f"wss://{ACCOUNT}.gremlin.cosmos.azure.com:443/"

# Username required by Azure Cosmos DB Gremlin API
USERNAME = f"/dbs/{DATABASE}/colls/{GRAPH}"
