import toml
import pandas as pd
from dotenv import load_dotenv
from os import getenv

import snowflake.snowpark.functions as sff
from snowflake.ml.feature_store import FeatureStore, CreationMode, Entity, FeatureView
from snowflake.snowpark.context import get_active_session

import utils.util_snowflake as us

#Reference
#https://github.com/ncl-icb-analytics/snowpark_testing/blob/main/FeatureStore.qmd

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

connection_params = {
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),
    "database": getenv("DATABASE"),
    "schema": "CANCER_CWT"
}

session = us.snowpark_session_create(connection_params)

# Add a query tag to the session. This helps with debugging and performance monitoring.
session.query_tag = {
    "origin":"sf_sit-is", 
    "name":"cwt_fs_overview", 
    "version":{"major":1, "minor":0}, 
    "attributes":{"is_quickstart":0, "source":"notebook"}
}

# Print the current role, warehouse, and database/schema
print(f"role: {session.get_current_role()} | WH: {session.get_current_warehouse()} | DB.SCHEMA: {session.get_fully_qualified_current_schema()}")

# Set up feature store
fs = FeatureStore(
    session=session,
    database=connection_params["database"],
    name="CANCER_CWT",
    default_warehouse=connection_params["warehouse"],
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
)

#Entity
entity_existing = fs.list_entities()

entity_record = Entity(
    name="RECORD",
    join_keys=["RECORDID"],
    desc="Record ID"
)

if entity_existing.filter(sff.col("NAME") == "RECORD").count() == 0:
    fs.register_entity(entity_record)

