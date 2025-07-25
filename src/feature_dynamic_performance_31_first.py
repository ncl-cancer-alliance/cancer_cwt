#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when, month, year, lit, coalesce, in_

#Utility script imports
import utils.util_snowflake as us

#Function to derive the 31 day performance figures (First Treatment)
def performance_31day_first(df):
    #Filter out to only valid 31 Day (First Treatment) records
    df = df.where(
        (in_([col("PATHWAY_CANCERTREATMENTEVENTTYPE")], ["01", "07", "12"])) &
        (col("PATHWAY_CANCERTREATMENTMODALITY") != 98) &
        not_(is_null(col("CWT_PRIMARYDIAGNOSIS_CODE"))) &
        not_(is_null(col("DATE_CANCERTREATMENTPERIODSTARTDATE"))) &
        not_(is_null(col("DATE_TREATMENTSTARTDATE")))        
    )

    #Set the Date fields
    date_field_col = "DATE_TREATMENTSTARTDATE"

    df = df.with_column(
        "PER_DATE_YEAR",
        year(col(date_field_col))
    )

    #Set the Date fields
    df = df.with_column(
        "PER_DATE_MONTH",
        month(col(date_field_col))
    )

    #Set the relevant organisation
    org_col = "ORG_ACCOUNTABLETREATING"
    df = df.with_column(
        "PER_ORG_SITE",
        df[org_col + "_SITE"]
    )
    df = df.with_column(
        "PER_ORG_TRUST",
        df[org_col + "_TRUST"]
    )

    df = df.with_column(
        "PER_ORG_NCL",
        df["GEO_TRUST_TREATMENTSTARTDATE"]
    )

    #Set the metric name
    df = df.with_column(
        "PER_METRIC",
        lit("31 Day")
    )

    #Calculate the 31 Day value
    df = df.with_column(
        "PER_VALUE",
        df["DATE_TREATMENTSTARTDATE"] - 
        df["DATE_CANCERTREATMENTPERIODSTARTDATE"] -
        coalesce(df["WTA_TREATMENTADJUSTMENT"], lit(0))
    )

    df = df.with_column(
        "PER_NUMERATOR",
        when(df["PER_VALUE"] <= 31, 0)
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
    "query_tag": "CANCER DYNAMIC TABLE FOR CWT PERFORMANCE 31 Day (First Treatment)",
    
    "session_database": getenv("DATABASE"),
    "session_schema": getenv("SCHEMA"),
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),

    "destination_database": getenv("DATABASE"),
    "destination_schema": getenv("SCHEMA"),
    "destination_table": "CWT_PERFORMANCE_31DAY_FIRST",
    
    "fdt_comment": "Calculates 31 Day Performance (First Treatment)"
}

us.create_dynamic_features(transformation_func=performance_31day_first, params=feature_dynamic_params)