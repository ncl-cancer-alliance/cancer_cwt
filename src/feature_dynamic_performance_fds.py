#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when, month, year, lit, coalesce, in_
from snowflake.ml.feature_store import FeatureView

#Utility script imports
import utils.util_snowflake as us

#Function to derive the 2ww performance figures
def performance_fds(df):

    #Calculate the PER_VALUE first since it is needed in the filter for valid records
    ##Determine which end date column to use
    df = df.with_column(
        "TEMP_FDSENDDATE",
        when((
                col("DATE_CANCERTREATMENTPERIODSTARTDATE") < 
                col("DATE_FDSPATHWAYENDDATE")
            ),
            col("DATE_CANCERTREATMENTPERIODSTARTDATE"))
        .otherwise(col("DATE_FDSPATHWAYENDDATE"))
    )

    df = df.with_column(
        "PER_VALUE",
        df["TEMP_FDSENDDATE"] - 
        df["DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"] -
        coalesce(df["WTA_FIRSTSEENADJUSTMENT"], lit(0))
    )

    #Filter out to only valid 2ww records
    df = df.where(
        (
            (in_([col("PATHWAY_FDPENDREASON")], ["01", "02", "04"])) |
            (
                (col("PATHWAY_FDPENDREASON") == "03") &
                (col("PATHWAY_FDPEXCLUSIONREASON") == "01") &
                (col("PER_VALUE") > 28)
            )
        ) &
        not_(is_null(col("DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"))) &
        not_(is_null(col("DATE_FDSPATHWAYENDDATE")))        
    )

    #Set the Date fields
    df = df.with_column(
        "PER_DATE_YEAR",
        year(col("DATE_FDSPATHWAYENDDATE"))
    )

    #Set the Date fields
    df = df.with_column(
        "PER_DATE_MONTH",
        month(col("DATE_FDSPATHWAYENDDATE"))
    )

    #Set the relevant organisation
    df = df.with_column(
        "PER_ORG",
        df["ORG_FDPEND"]
    )

    df = df.with_column(
        "PER_ORG_NCL",
        df["GEO_TRUST_FDS"]
    )

    #Set the metric name
    df = df.with_column(
        "PER_METRIC",
        lit("FDS")
    )

    #Set the numerator (breaches) and denominator (all patients)
    df = df.with_column(
        "PER_NUMERATOR",
        when(df["PER_VALUE"] <= 28, 0)
        .otherwise(1)
    )

    df = df.with_column(
        "PER_DENOMINATOR",
        lit(1)
    )

    #Remove unused columns
    df = df[["RECORD_ID", "PER_DATE_YEAR", "PER_DATE_MONTH", 
            "PER_ORG", "PER_ORG_NCL", "PER_METRIC", 
            "PER_VALUE", "PER_NUMERATOR", "PER_DENOMINATOR"]]

    print("Sample of output:")
    df.show()

    return df

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

feature_dynamic_params = {
    "base_table": "CWT_BASE",
    "query_tag": "CANCER DYNAMIC TABLE FOR CWT PERFORMANCE FDS",
    
    "session_database": getenv("DATABASE"),
    "session_schema": getenv("SCHEMA"),
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),

    "destination_database": getenv("DATABASE"),
    "destination_schema": getenv("SCHEMA"),
    "destination_table": "CWT_PERFORMANCE_FDS",
    
    "fdt_comment": "Calculates FDS Performance"
}

us.create_dynamic_features(transformation_func=performance_fds, params=feature_dynamic_params)