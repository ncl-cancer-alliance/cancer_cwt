#General imports
import toml
from dotenv import load_dotenv
from os import getenv

from snowflake.ml.feature_store import FeatureStore, CreationMode

#Utility script imports
import utils.util_snowflake as us

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

#Set the name of the Feature Store


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
    connection_params, "CANCER CWT Feature Store Creation")

#Create the feature store
fs = FeatureStore(
    session=session,
    database=connection_params["database"],
    name="CANCER_CWT",
    default_warehouse=getenv("WAREHOUSE"),
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST
)