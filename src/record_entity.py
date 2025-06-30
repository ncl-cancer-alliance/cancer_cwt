#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
import snowflake.snowpark.functions as sff
from snowflake.ml.feature_store import Entity

#Utility script imports
import utils.util_snowflake as us

#Reference
#https://github.com/ncl-icb-analytics/snowpark_testing/blob/main/FeatureStore.qmd

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

#Create a Snowflake session
connection_params = {
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),
    "database": getenv("DATABASE"),
    "schema": "CANCER_CWT"
}

session = us.snowpark_session_create(
    connection_params, "CANCER CWT RECORD ENTITY")

#Load the feature store
fs = us.load_feature_store(
    session=session,
    database=connection_params["database"],
    name="CANCER_CWT"
)

#Entity
entity_existing = fs.list_entities()

entity_record = Entity(
    name="CANCER_CWT_RECORD",
    join_keys=["RECORD_ID"],
    desc="Cancer CWT data Records"
)

fs.register_entity(entity_record)