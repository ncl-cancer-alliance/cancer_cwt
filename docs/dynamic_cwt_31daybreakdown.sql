CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.CANCER_CWT.CWT_31DAYBREAKDOWN (
    RECORD_ID VARCHAR,
	D31_FIRST BOOLEAN,
	D31_BREAKDOWN VARCHAR
)
COMMENT="Breakdown for 31 Day pathway records."
TARGET_LAG = "2 hours"
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT RECORD_ID, True, 'First' 
FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER_CWT.CWT_PERFORMANCE_31DAY_FIRST
UNION ALL
SELECT RECORD_ID, False,  D31_BREAKDOWN 
FROM DATA_LAB_NCL_TRAINING_TEMP.CANCER_CWT.CWT_PERFORMANCE_31DAY_SUBSEQUENT