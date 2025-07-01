#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when, lit, coalesce
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

    #Set the relevant organisation
    df = df.with_column(
        "PER_ORG",
        df["ORG_FIRSTSEEN"]
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
    df = df[["RECORD_ID", "PER_ORG", "PER_METRIC", 
            "PER_VALUE", "PER_NUMERATOR", "PER_DENOMINATOR"]]

    print("Sample of output:")
    df.show()

    return df

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
    connection_params, "CANCER CWT FEATURE VIEW 2WW")

#Load the feature store
fs = us.load_feature_store(
    session=session,
    database=connection_params["database"],
    name="CANCER_CWT"
)

#Load the relevant entities
entity_record = us.load_entity(fs, "CANCER_CWT_RECORD")

#Load the base data
df_base = session.table("CWT_BASE")

#Register the feature view
cwt_pathway_fv = FeatureView(
   name="CWT_PERFORMANCE",  # name of feature view
   entities=[entity_record],  # entities
   feature_df=performance_2ww(df_base),  # definition query
   #timestamp_col="TS",  # timestamp column
   refresh_freq="30 days",  # refresh frequency
   desc="Table of performance metrics for CWT data",
   refresh_mode="incremental"
).attach_feature_desc(
   {
       "PER_ORG": "Associated organisation for the metric",
       "PER_METRIC": "Performance metric name",
       "PER_VALUE": "Numeric value of the metric",
       "PER_NUMERATOR": "Whether the value is a breach (and quantity)",
       "PER_DENOMINATOR": "Denominator of patients for aggregating"
   }
)

cwt_pathway_fv = fs.register_feature_view(
    cwt_pathway_fv, version="1", overwrite=True)