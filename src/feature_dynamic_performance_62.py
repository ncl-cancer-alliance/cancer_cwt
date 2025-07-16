#General imports
import toml
from dotenv import load_dotenv
from os import getenv

#Snowflake imports
from snowflake.snowpark.functions import col, is_null, not_, when, month, year, lit, coalesce, in_
from snowflake.snowpark import Row

#Utility script imports
import utils.util_snowflake as us

#Function to derive the 31 day performance figures (First Treatment)
def performance_62day(df):

    #Define Upgrade pathway as some logic is dependent on it
    pathway_upgrade = (
        (col("PATHWAY_SOURCEOFREFERRALFOROUTPATIENT") != 17) &
        not_(is_null(col("DATE_CONSULTANTUPGRADEDATE")))
    )
     
    #Filter out to only valid 62 Day records
    df = df.where(
        (in_([col("PATHWAY_CANCERTREATMENTEVENTTYPE")], ["01", "07", "12"])) &
        (col("PATHWAY_CANCERTREATMENTMODALITY") != 98) &
        not_(is_null(col("CWT_PRIMARYDIAGNOSISICD"))) &
        not_(is_null(col("DATE_CANCERTREATMENTPERIODSTARTDATE"))) &
        not_(is_null(col("DATE_TREATMENTSTARTDATE"))) &
        #Additional requirement
        #(For all USC, Screening activity; First Seen Org is required)
        (not_(is_null(col("ORG_FIRSTSEEN_TRUST"))) | pathway_upgrade)
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

    #Set the metric name
    df = df.with_column(
        "PER_METRIC",
        lit("62 Day")
    )

    #Calculate the 62 Day value
    df = df.with_column(
        "PER_VALUE",
        #If USC, Breast Symptomatic, Screening
        when(
            not_(pathway_upgrade),
            df["DATE_TREATMENTSTARTDATE"] - 
            df["DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"] -
            coalesce(df["WTA_FIRSTSEENADJUSTMENT"], lit(0)) -
            coalesce(df["WTA_TREATMENTADJUSTMENT"], lit(0))
        )
        #If Upgrade then calculation depends if the upgrade date is before or 
        # on the date first seen
        .when(
            df["DATE_CONSULTANTUPGRADEDATE"] <= df["DATE_DATEFIRSTSEEN"],
            df["DATE_TREATMENTSTARTDATE"] - 
            df["DATE_CONSULTANTUPGRADEDATE"] -
            coalesce(df["WTA_FIRSTSEENADJUSTMENT"], lit(0))  -
            coalesce(df["WTA_TREATMENTADJUSTMENT"], lit(0))
        )
        .otherwise(
            df["DATE_TREATMENTSTARTDATE"] - 
            df["DATE_CONSULTANTUPGRADEDATE"] -
            coalesce(df["WTA_TREATMENTADJUSTMENT"], lit(0))
        )
    )

    df = df.with_column(
        "PER_NUMERATOR",
        when(df["PER_VALUE"] <= 62, 0.0)
        .otherwise(1.0)
    )

    df = df.with_column(
        "PER_DENOMINATOR",
        lit(1.0)
    )

    #Determine Accountable Investigating Provider
    df = df.with_column(
        "D62_ACC_DIAGNOSTIC",
        coalesce(
            col("ORG_ACCOUNTABLEINVESTIGATING_TRUST"),
            col("ORG_CONSULTANTUPGRADE_TRUST"),
            col("ORG_FIRSTSEEN_TRUST")
        )
    )

    df = df.with_column(
        "D62_ACC_TREATMENT",
        col("ORG_ACCOUNTABLETREATING_TRUST")
    )

    #Determine allocation method
    df = df.with_column(
        "D62_ALLOCATIONMETHOD",
        when(
            col("D62_ACC_DIAGNOSTIC") == col("D62_ACC_TREATMENT"),
            "Solo")
        .when(
            is_null(col("ORG_ACCOUNTABLEINVESTIGATING_TRUST")),
            "5050")
        .otherwise("6 Scenarios")
    )

    #Pre-emptively set 6S Scenario to Null so I only have to define it for 6s
    df = df.with_column(
        "D62_6S_SCENARIO",
        lit(None)
    )

    #Split data by allocation method
    df_solo = df.filter(col("D62_ALLOCATIONMETHOD") == "Solo") \
        .select(df.columns)
    
    df_5050 = df.filter(col("D62_ALLOCATIONMETHOD") == "5050") \
        .select(df.columns)
    
    df_6s = df.filter(col("D62_ALLOCATIONMETHOD") == "6 Scenarios") \
        .select(df.columns)
    
    #Solo##################################################

    #Set the org columns
    df_solo = df_solo.with_column(
        "PER_ORG_TRUST",
        col("D62_ACC_DIAGNOSTIC")
    )

    df_solo = df_solo.with_column(
        "PER_ORG_SITE",
        lit(None)
    )

    df_solo = df_solo.with_column(
        "PER_ORG_NCL",
        df_solo["GEO_TRUST_TREATMENTSTARTDATE"]
    )

    #5050##################################################

    #Set the value
    df_5050 = df_5050.with_column(
        "PER_NUMERATOR",
        col("PER_NUMERATOR") * 0.5
    )

    df_5050 = df_5050.with_column(
        "PER_DENOMINATOR",
        col("PER_DENOMINATOR") * 0.5
    )

    df_5050_diag = df_5050

    df_5050_diag = df_5050_diag.with_column(
        "PER_ORG_TRUST",
        col("D62_ACC_DIAGNOSTIC")
    )

    df_5050_diag = df_5050_diag.with_column(
        "PER_ORG_SITE",
        lit(None)
    )

    df_5050_diag = df_5050_diag.with_column(
        "PER_ORG_NCL",
        coalesce(
            col("GEO_TRUST_ACCOUNTABLEINVESTIGATING"),
            col("GEO_TRUST_CONSULTANTUPGRADE"),
            col("GEO_TRUST_DATEFIRSTSEEN")
        )
    )

    df_5050_treat = df_5050

    df_5050_treat = df_5050_treat.with_column(
        "PER_ORG_TRUST",
        col("D62_ACC_TREATMENT")
    )

    df_5050_treat = df_5050_treat.with_column(
        "PER_ORG_SITE",
        lit(None)
    )

    df_5050_treat = df_5050_treat.with_column(
        "PER_ORG_NCL",
        df_solo["GEO_TRUST_TREATMENTSTARTDATE"]
    )

    df_5050 = df_5050_diag.union_all(df_5050_treat)

    #6 Scenarios#############################################

    #Calculate 38 Day and 24 Day Performance
    df_6s = df_6s.with_column(
        "PER_38D",
        #If USC, Breast Symptomatic, Screening
        when(
            not_(pathway_upgrade),
            df_6s["DATE_TRANSFERTOTREATMENTDATE"] - 
            df_6s["DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE"] -
            coalesce(df_6s["WTA_FIRSTSEENADJUSTMENT"], lit(0))
        )
        #If Upgrade then calculation depends if the upgrade date is before or 
        # on the date first seen
        .when(
            df_6s["DATE_CONSULTANTUPGRADEDATE"] <= df_6s["DATE_DATEFIRSTSEEN"],
            df_6s["DATE_TRANSFERTOTREATMENTDATE"] - 
            df_6s["DATE_CONSULTANTUPGRADEDATE"] -
            coalesce(df_6s["WTA_FIRSTSEENADJUSTMENT"], lit(0)) 
        )
        .otherwise(
            df_6s["DATE_TRANSFERTOTREATMENTDATE"] - 
            df_6s["DATE_CONSULTANTUPGRADEDATE"]
        )
    )

    df_6s = df_6s.with_column(
            "PER_24D",
            df_6s["DATE_TREATMENTSTARTDATE"] - 
            df_6s["DATE_TRANSFERTOTREATMENTDATE"] -
            coalesce(df_6s["WTA_TREATMENTADJUSTMENT"], lit(0))
    )

    #Determine Scenario

    #Boolean shorthand to determine breaches
    d62 = (col("PER_VALUE") <= 62)
    d38 = (col("PER_38D") <= 38)
    d24 = (col("PER_24D") <= 24)
    nd62 = not_(d62)
    nd38 = not_(d38)
    nd24 = not_(d24)

    #Dictionary containing patient allocation based on the organisation
    dict_6s = {
        "diag":{
            1:{"num": 0.5, "den":0.5},
            2:{"num": 0.5, "den":0.5},
            3:{"num": 0,   "den":0},
            4:{"num": 0,   "den":0},
            5:{"num": 0,   "den":1},
            6:{"num": 0,   "den":0.5},
        },
        "treat":{
            1:{"num": 0.5, "den":0.5},
            2:{"num": 0.5, "den":0.5},
            3:{"num": 1,   "den":1},
            4:{"num": 0,   "den":1},
            5:{"num": 0,   "den":0},
            6:{"num": 0,   "den":0.5},
        }
    }

    #Determine which scenario
    df_6s = df_6s.with_column(
        "D62_6S_SCENARIO",
        when( d62  & d38  & d24,  1)
        .when(d62  & d38  & nd24, 2)
        .when(d62  & nd38 & d24,  3)
        .when(nd62 & d38  & nd24, 4)
        .when(nd62 & nd38 & d24,  5)
        .when(nd62 & nd38 & nd24, 6)
    )

    #Diagnostic 6S
    df_6s_diag = df_6s

    df_6s_diag = df_6s_diag.with_column(
        "PER_ORG_TRUST",
        df_6s_diag["D62_ACC_DIAGNOSTIC"]
    )

    df_6s_diag = df_6s_diag.with_column(
        "PER_ORG_SITE",
        lit(None)
    )

    df_6s_diag = df_6s_diag.with_column(
        "PER_ORG_NCL",
        coalesce(
            col("GEO_TRUST_ACCOUNTABLEINVESTIGATING"),
            col("GEO_TRUST_CONSULTANTUPGRADE"),
            col("GEO_TRUST_DATEFIRSTSEEN")
        )
    )

    #Create a copy for the 38 Day Performance
    df_6s_diag_d38 = df_6s_diag

    #Calculate patient allocation
    df_6s_diag = df_6s_diag.with_column(
        "PER_NUMERATOR",
        when(df_6s_diag["D62_6S_SCENARIO"] == 1, dict_6s["diag"][1]["num"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 2, dict_6s["diag"][2]["num"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 3, dict_6s["diag"][3]["num"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 4, dict_6s["diag"][4]["num"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 5, dict_6s["diag"][5]["num"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 6, dict_6s["diag"][6]["num"])
    )

    df_6s_diag = df_6s_diag.with_column(
        "PER_DENOMINATOR",
        when(df_6s_diag["D62_6S_SCENARIO"] == 1, dict_6s["diag"][1]["den"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 2, dict_6s["diag"][2]["den"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 3, dict_6s["diag"][3]["den"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 4, dict_6s["diag"][4]["den"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 5, dict_6s["diag"][5]["den"])
        .when(df_6s_diag["D62_6S_SCENARIO"] == 6, dict_6s["diag"][6]["den"])
    )

    #Treatment 6S
    df_6s_treat = df_6s

    df_6s_treat = df_6s_treat.with_column(
        "PER_ORG_TRUST",
        df_6s_treat["D62_ACC_TREATMENT"]
    )

    df_6s_treat = df_6s_treat.with_column(
        "PER_ORG_SITE",
        df_6s_treat["ORG_ACCOUNTABLETREATING_SITE"]
    )

    df_6s_treat = df_6s_treat.with_column(
        "PER_ORG_NCL",
        df_solo["GEO_TRUST_TREATMENTSTARTDATE"]
    )

    #Create a copy for the 38 Day Performance
    df_6s_treat_d24 = df_6s_treat

    #Calculate patient allocation
    df_6s_treat = df_6s_treat.with_column(
        "PER_NUMERATOR",
        when(df_6s_treat["D62_6S_SCENARIO"] == 1, dict_6s["treat"][1]["num"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 2, dict_6s["treat"][2]["num"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 3, dict_6s["treat"][3]["num"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 4, dict_6s["treat"][4]["num"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 5, dict_6s["treat"][5]["num"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 6, dict_6s["treat"][6]["num"])
    )

    df_6s_treat = df_6s_treat.with_column(
        "PER_DENOMINATOR",
        when(df_6s_treat["D62_6S_SCENARIO"] == 1, dict_6s["treat"][1]["den"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 2, dict_6s["treat"][2]["den"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 3, dict_6s["treat"][3]["den"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 4, dict_6s["treat"][4]["den"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 5, dict_6s["treat"][5]["den"])
        .when(df_6s_treat["D62_6S_SCENARIO"] == 6, dict_6s["treat"][6]["den"])
    )

    #Calculate 38 Day Performance
    df_6s_diag_d38 = df_6s_diag_d38.with_column(
        "PER_METRIC",
        lit("38 Day")
    )

    df_6s_diag_d38 = df_6s_diag_d38.with_column(
        "PER_VALUE",
        col("PER_38D")
    )

    df_6s_diag_d38 = df_6s_diag_d38.with_column(
        "PER_NUMERATOR",
        when(d38, 0)
        .otherwise(1)
    )

    df_6s_diag_d38 = df_6s_diag_d38.with_column(
        "PER_DENOMINATOR",
        lit(1)
    )

    #Calculate 24 Day Performance
    df_6s_treat_d24 = df_6s_treat_d24.with_column(
        "PER_METRIC",
        lit("24 Day")
    )

    df_6s_treat_d24 = df_6s_treat_d24.with_column(
        "PER_VALUE",
        col("PER_24D")
    )

    df_6s_treat_d24 = df_6s_treat_d24.with_column(
        "PER_NUMERATOR",
        when(d24, 0)
        .otherwise(1)
    )

    df_6s_treat_d24 = df_6s_treat_d24.with_column(
        "PER_DENOMINATOR",
        lit(1)
    )

    df_6s = df_6s_diag.union_all(df_6s_treat)

    df_6s_extra = df_6s_diag_d38.union_all(df_6s_treat_d24)

    #Remove unused columns
    d62_cols = ["RECORD_ID", "PER_DATE_YEAR", "PER_DATE_MONTH", 
        "PER_ORG_TRUST", "PER_ORG_SITE", "PER_ORG_NCL", "PER_METRIC", 
        "PER_VALUE", "PER_NUMERATOR", "PER_DENOMINATOR",
        "D62_ACC_DIAGNOSTIC", "D62_ACC_TREATMENT", 
        "D62_ALLOCATIONMETHOD", "D62_6S_SCENARIO"]
    
    df_solo = df_solo.select(d62_cols)
    df_5050 = df_5050.select(d62_cols)
    df_6s = df_6s.select(d62_cols)
    df_6s_extra = df_6s_extra.select(d62_cols)
    
    df_out = df_solo.union_all(df_5050).union_all(df_6s)

    df_out = df_out.union_all(df_6s_extra)

    #Outstanding
    #Get the NCL flag for diagnostic orgs
    #Apply 6s allocation
    #Move solo, 5050, 6s allocation to their own functions

    print("Sample of output:")

    df_out.show()

    return df_out

#Load env settings
load_dotenv(override=True)
config = toml.load("config.toml")

feature_dynamic_params = {
    "base_table": "CWT_BASE",
    "query_tag": "CANCER DYNAMIC TABLE FOR CWT PERFORMANCE 62 Day",
    
    "session_database": getenv("DATABASE"),
    "session_schema": getenv("SCHEMA"),
    "account": getenv("ACCOUNT"),
    "user": getenv("USER"),
    "authenticator": getenv("AUTHENTICATOR"),
    "role": getenv("ROLE"),
    "warehouse": getenv("WAREHOUSE"),

    "destination_database": getenv("DATABASE"),
    "destination_schema": getenv("SCHEMA"),
    "destination_table": "CWT_PERFORMANCE_62DAY",
    
    "fdt_comment": "Calculates 62 Day Performance"
}

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
    connection_params, "Build 62 Day Performance table")

#Load the base data
df_base = session.table("CWT_BASE")

us.create_dynamic_features(transformation_func=performance_62day, params=feature_dynamic_params)