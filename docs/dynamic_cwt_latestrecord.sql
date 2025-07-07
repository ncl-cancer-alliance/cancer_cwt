--No longer used as integrated into the base
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.CANCER_CWT.CWT_LATESTSUBMISSION (
    --Entry identifiers
    SK VARCHAR, --UUID for rows in the CWT0001 Source table
    --Metadata
    META_LATESTRECORD NUMBER -- 1 if the latest entry for a record, 0 otherwise
)
COMMENT="Dynamic table lookup for the latest entry for each record"
TARGET_LAG = "2 hours"
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT 
SK, 
CASE
    WHEN cwt."UniqSubmissionID" = MAX(cwt."UniqSubmissionID") OVER (PARTITION BY cwt.RECORDID) THEN 1
    ELSE 0
END AS META_LATESTRECORD

FROM "Data_Store_Waiting".CWTDS."CWT001Data" cwt