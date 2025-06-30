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
        (col("PATHWAY_PRIORITYTYPECODE") == 3) &
        (col("PATHWAY_REFERRALTYPE") != 16) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") != 17) &
        is_null(col("DATE_CONSULTANTUPGRADEDATE")) &
        #Additional field
        not_(is_null(col("ORG_FIRSTSEEN")))
    )

    pathway_breastsymp = (
        (col("PATHWAY_PRIORITYTYPECODE") == 3) &
        (col("PATHWAY_REFERRALTYPE") == 16) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") != 17) &
        is_null(col("DATE_CONSULTANTUPGRADEDATE")) &
        #Additional field
        not_(is_null(col("ORG_FIRSTSEEN")))
    )

    pathway_screening = (
        (col("PATHWAY_PRIORITYTYPECODE") == 2) &
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") == 17) &
        #Additional field
        not_(is_null(col("ORG_FIRSTSEEN")))
    )

    pathway_upgrade = (
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") != 17) &
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

#Load the relevant entities
entity_record = us.load_entity(fs, "CANCER_CWT_RECORD")

#Load the base data
df_base = session.table("CWT_BASE")

#Register the dataframe
cwt_pathway_fv = FeatureView(
   name="CWT_PATHWAY",  # name of feature view
   entities=[entity_record],  # entities
   feature_df=determine_pathway(df_base),  # definition query
   #timestamp_col="TS",  # timestamp column
   refresh_freq="30 days",  # refresh frequency
   desc="Determine the pathway for a entry",
   refresh_mode="incremental"
).attach_feature_desc(
   {
       "PATHWAY": "Categorises data into USC, Breast Symptomatic, Screening, Upgrade, Unknown"
   }
)

cwt_pathway_fv = fs.register_feature_view(cwt_pathway_fv, version="1", overwrite=True)