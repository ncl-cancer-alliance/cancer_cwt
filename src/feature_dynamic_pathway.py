#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when
from snowflake.ml.feature_store import FeatureView

#Utility script imports
import utils.util_snowflake as us

def determine_pathway(df):
    
    """
    Function to get the pathway for a given entry.
    df: Dataframe containing the base CWT data
    Returns:
        - df: Dataframe containing the target features
    """
    
    pathway_usc = (
        (col("PATHWAY_PRIORITYTYPE_CODE") == 3) &
        (col("CWT_CANCERREFERALTYPE_CODE") != 16) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE") != 17) &
        is_null(col("DATE_CONSULTANTUPGRADEDATE"))
    )

    pathway_breastsymp = (
        (col("PATHWAY_PRIORITYTYPE_CODE") == 3) &
        (col("CWT_CANCERREFERALTYPE_CODE") == 16) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE") != 17) &
        is_null(col("DATE_CONSULTANTUPGRADEDATE"))
    )

    pathway_screening = (
        (col("PATHWAY_PRIORITYTYPE_CODE") == 2) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE") == 17)
    )

    pathway_upgrade = (
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE") != 17) &
        not_(is_null(col("DATE_CONSULTANTUPGRADEDATE")))
    )

    df = df.with_column(
        "PATHWAY",
        when(pathway_usc, "USC")
        .when(pathway_breastsymp, "Breast Symptomatic")
        .when(pathway_screening, "Screening")
        .when(pathway_upgrade, "Upgrade")
        .otherwise("Unknown")
    )


    df = df[["RECORD_ID", "PATHWAY"]]

    return df

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

feature_dynamic_params = {
    "base_table": "CWT_BASE",
    "query_tag": "CANCER DYNAMIC TABLE FOR CWT PATHWAY",
    
    "session_database": getenv("DATABASE"),
    "session_schema": getenv("SCHEMA"),
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),

    "destination_database": getenv("DATABASE"),
    "destination_schema": getenv("SCHEMA"),
    "destination_table": "CWT_PATHWAY",
    
    "fdt_comment": "Categorises data into USC, Breast Symptomatic, Screening, Upgrade, Unknown"
}

us.create_dynamic_features(transformation_func=determine_pathway, params=feature_dynamic_params)