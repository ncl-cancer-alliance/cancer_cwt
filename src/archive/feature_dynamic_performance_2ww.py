#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when, month, year, lit, coalesce
from snowflake.ml.feature_store import FeatureView

#Utility script imports
import utils.util_snowflake as us

#Function to derive the 2ww performance figures
def performance_2ww(df):
    #Filter out to only valid 2ww records
    df = df.where(
        (col("PATHWAY_PRIORITYTYPECODE") == 3) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") != 17) &
        not_(is_null(col("DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"))) &
        not_(is_null(col("DATE_DATEFIRSTSEEN")))        
    )

    #Set the Date fields
    df = df.with_column(
        "PER_DATE_YEAR",
        year(col("DATE_DATEFIRSTSEEN"))
    )

    #Set the Date fields
    df = df.with_column(
        "PER_DATE_MONTH",
        month(col("DATE_DATEFIRSTSEEN"))
    )

    #Set the relevant organisation
    df = df.with_column(
        "PER_ORG_TRUST",
        df["ORG_FIRSTSEEN_TRUST"]
    ) 

    df = df.with_column(
        "PER_ORG_SITE",
        df["ORG_FIRSTSEEN_SITE"]
    )

    df = df.with_column(
        "PER_ORG_NCL",
        df["GEO_TRUST_DATEFIRSTSEEN"]
    )

    #Set the metric name
    df = df.with_column(
        "PER_METRIC",
        lit("2WW")
    )

    #Calculate the 2ww value
    df = df.with_column(
        "PER_VALUE",
        df["DATE_DATEFIRSTSEEN"] - 
        df["DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"] -
        coalesce(df["WTA_FIRSTSEENADJUSTMENT"], lit(0))
    )

    df = df.with_column(
        "PER_NUMERATOR",
        when(df["PER_VALUE"] <= 14, 0)
        .otherwise(1)
    )

    df = df.with_column(
        "PER_DENOMINATOR",
        lit(1)
    )

    #Remove unused columns
    df = df[["RECORD_ID", "PER_DATE_YEAR", "PER_DATE_MONTH", 
            "PER_ORG_TRUST", "PER_ORG_SITE", "PER_ORG_NCL", "PER_METRIC", 
            "PER_VALUE", "PER_NUMERATOR", "PER_DENOMINATOR"]]

    print("Sample of output:")
    df.show()

    return df

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

feature_dynamic_params = {
    "base_table": "CWT_BASE",
    "query_tag": "CANCER DYNAMIC TABLE FOR CWT PERFORMANCE 2WW",
    
    "session_database": getenv("DATABASE"),
    "session_schema": getenv("SCHEMA"),
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),

    "destination_database": getenv("DATABASE"),
    "destination_schema": getenv("SCHEMA"),
    "destination_table": "CWT_PERFORMANCE_2WW",
    
    "fdt_comment": "Calculates 2WW Performance"
}

us.create_dynamic_features(transformation_func=performance_2ww, params=feature_dynamic_params)