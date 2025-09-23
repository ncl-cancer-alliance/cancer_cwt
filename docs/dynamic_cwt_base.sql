CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__CWT_PATHWAY.CWT_BASE (

    --Description: Cleaned dataset for the CWT Pathway level data
    --Author: Jake Kealey

    --Entry identifiers
    SK_CWT_ID VARCHAR, --UUID for rows in the CWT0001 Source table
    RECORD_ID VARCHAR, --UUID for the referral (Not unique)
    PSUEDO_NHS_ID VARCHAR, --NHS Pseudo for the record

    --Organisation Fields
    ORG_ACCOUNTABLEINVESTIGATING_TRUST VARCHAR, --Investigating org for 6 Scernarios
    ORG_CONSULTANTUPGRADE_TRUST VARCHAR,
    ORG_CONSULTANTUPGRADE_SITE VARCHAR, --Investigating org for Upgrade pathway
    ORG_FIRSTSEEN_TRUST VARCHAR,
    ORG_FIRSTSEEN_SITE VARCHAR, --Org where patient is first seen post referral
    ORG_FDPEND_TRUST VARCHAR,
    ORG_FDPEND_SITE VARCHAR, --Org where the FD Pathway ends
    ORG_PATIENTPATHWAYIDENTIFIERISSUER_TRUST VARCHAR, --Org associated with the pathway ID
    ORG_ACCOUNTABLETREATING_TRUST VARCHAR,
    ORG_ACCOUNTABLETREATING_SITE VARCHAR, --Treating Organisation

    --Breakdowns Fields
    ---Primary Diagnosis---
    CWT_PRIMARYDIAGNOSIS_CODE VARCHAR, --Diagnosis code
    CWT_PRIMARYDIAGNOSIS_DESC VARCHAR, --Diagnosis description
    CWT_PRIMARYDIAGNOSIS_GROUPING VARCHAR, --Grouping
    CWT_PRIMARYDIAGNOSIS_COSD_STAGEABLE BOOLEAN, --COSD
    CWT_PRIMARYDIAGNOSIS_RCRD_STAGEABLE BOOLEAN, --RCRD

    ---Referral Type---
    CWT_CANCERREFERALTYPE_CODE CHAR(2), --Referral Type Code
    CWT_CANCERREFERALTYPE_DESC VARCHAR, --Referral Type Description

    ---Modality---
    CWT_MODALITY_DESC VARCHAR, --Modality Description

    --Patient Status--
    CWT_PATIENTSTATUS_CODE CHAR(2), --Patient Status (on pathway)

    --GP Fields
    REG_ICB_CODE CHAR(3),
    REG_PRACTICE_CODE CHAR(8), --GP Practice Code
    
    --Residence Fields
    RES_ICB_CODE CHAR(3), --ICB of Residence
    RES_LA_CODE CHAR(9), --Local Authority of Residence
    RES_LSOA CHAR(9), --LSOA of Residence
    
    --Pathway Identifiers
    PATHWAY_PRIORITYTYPE_CODE NUMBER, --Identifier for pathway
    PATHWAY_CANCERTREATMENTEVENTTYPE_CODE CHAR(2), --Identifier for pathway
    PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE CHAR(2), --Identifier for pathway
    PATHWAY_CANCERTREATMENTMODALITY_CODE CHAR(2), --Identifier for pathway
    PATHWAY_CANCERCARESETTINGTREATMENT_CODE CHAR(2), --Identifier for admitted care
    PATHWAY_FDPENDREASON_CODE CHAR(2), --End reason used in FDP Logic
    PATHWAY_FDPEXCLUSIONREASON_CODE CHAR(2), --Exclusion reason for FDP
    PATHWAY_FDPOUTCOMEMETHOD_CODE CHAR(2),  --FDP Outcome Method
    PATHWAY_FDPOUTCOMEPROFTYPE_CODE CHAR(3), --FDP Outcome Prof Type

    --Date Fields
    DATE_DECISIONTOREFERDATE DATE, --Unused?
    DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE DATE, --Date of referral
    DATE_CONSULTANTUPGRADEDATE DATE, --Date for start for consultant pathway
    DATE_DATEFIRSTSEEN DATE, --Date of first completed appointment post referral 
    DATE_FDSPATHWAYENDDATE DATE, --Date where cancer is confirmed or ruled out
    DATE_TRANSFERTOTREATMENTDATE DATE, --Date between Investigation and Treatment windows
    DATE_CANCERTREATMENTPERIODSTARTDATE DATE, --Date for 31 Clock Start
    DATE_TREATMENTSTARTDATE DATE, --Date for Treatment starts
    
    --Waiting Time Adjustment
    WTA_FIRSTSEENADJUSTMENT NUMBER, --Adjustment for First Seen TTE
    WTA_TREATMENTADJUSTMENT NUMBER, --Adjustment for Treatment TTE
    WTA_TREATMENTREASON VARCHAR, --Reason for adjustment for Treatment TTE
    
    --Geo
    IS_GEO_GP BOOLEAN, --Flag for registered to a NCL GP
    IS_GEO_RESIDENCE BOOLEAN, --Flag for NCL residents
    IS_GEO_TRUST BOOLEAN, --Flag for interacting with a NCL Trust
    IS_GEO_TRUST_DATEFIRSTSEEN BOOLEAN, --Flag if the Date First Seen Organisation is a NCL Trust
    IS_GEO_TRUST_FDS BOOLEAN, --Flag if the FDS Organisation is a NCL Trust
    IS_GEO_TRUST_TREATMENTSTARTDATE BOOLEAN, --Flag if the Treatment Start Date Organisation is a NCL Trust
    IS_GEO_TRUST_ACCOUNTABLEINVESTIGATING BOOLEAN, --Flag if the Accountable Investigating Organisation is a NCL trust
    IS_GEO_TRUST_CONSULTANTUPGRADE BOOLEAN, --Flag if the Consultant Upgrade Organisation is a NCL trust

    --Record Events
    IS_EVENT_DATEFIRSTSEEN BOOLEAN, --Flag if the record includes the 2WW Pathway
    IS_EVENT_CANCERTREATMENTPERIOD BOOLEAN, --Flag if the record includes the 31D Pathway
    IS_EVENT_FDS BOOLEAN, --Flag if the record includes the 28D Pathway
    IS_EVENT_TREATMENTSTARTDATE BOOLEAN, --Flag if the record includes the 62D Pathway

    --Metadata
    META_SUBMISSIONID NUMBER --Numeric ID for the upload batch in the source

)
COMMENT="Dynamic table to use as an univserial base for CWT data."
TARGET_LAG = "24 hours"
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH org_par AS (
    SELECT 
        child."Organisation_Code" AS "ORG_SITE", 
        CASE 
            WHEN child."SK_OrganisationTypeID" = 41
            THEN child."Organisation_Code"
            ELSE par."Organisation_Code" 
        END AS "ORG_TRUST"
    FROM "Dictionary"."dbo"."Organisation" child

    LEFT JOIN "Dictionary"."dbo"."Organisation" par 
    ON child."SK_OrganisationID_ParentOrg" = par."SK_OrganisationID"
)

SELECT
    --Entry identifiers
    cwt.SK AS SK_CWT_ID,
    cwt.RECORDID AS RECORD_ID,
    cwt."NHS_NUMBER Pseudo" AS PSUEDO_NHS_ID,

    --Organisation Fields
    cwt.ACCOUNTABLEINVESTIGATINGPROVIDER AS ORG_ACCOUNTABLEINVESTIGATING_TRUST,

    org_cu.ORG_TRUST AS ORG_CONSULTANTUPGRADE_TRUST,
    cwt.ORGCONSUPGRADE AS ORG_CONSULTANTUPGRADE_SITE,

    org_fs.ORG_TRUST AS ORG_FIRSTSEEN_TRUST,
    cwt.ORGFIRSTSEEN AS ORG_FIRSTSEEN_SITE,

    org_fdp.ORG_TRUST AS ORG_FDPEND_TRUST,
    cwt.ORGFDPEND AS ORG_FDPEND_SITE,
    
    COALESCE(org_pi.ORG_TRUST, cwt.ORGPPI)  
    AS ORG_PATIENTPATHWAYIDENTIFIERISSUER_TRUST,

    org_at.ORG_TRUST AS ORG_ACCOUNTABLETREATING_TRUST,
    cwt.ORGTREATSTART AS ORG_ACCOUNTABLETREATING_SITE,
    
    --Breakdowns Fields
    ---Primary Diagnosis---
    cwt.PRIMARYDIAGNOSISICD AS CWT_PRIMARYDIAGNOSIS_CODE,
    ref_pd.FULL_DESCRIPTION AS CWT_PRIMARYDIAGNOSIS_DESC, 
    ref_pd.GROUPING AS CWT_PRIMARYDIAGNOSIS_GROUPING,
    CASE ref_pd.COSD_STAGEABLE 
        WHEN 'Stageable' THEN TRUE
        ELSE FALSE
    END AS CWT_PRIMARYDIAGNOSIS_COSD_STAGEABLE,
    CASE ref_pd.RCRD_STAGEABLE 
        WHEN 'Stageable' THEN TRUE
        ELSE FALSE
    END AS CWT_PRIMARYDIAGNOSIS_RCRD_STAGEABLE,
    
    ---Cancer Referral Type---
    cwt.REFTYPE AS CWT_CANCERREFERALTYPE_CODE,
    ref_rt.FULL_DESCRIPTION AS CWT_CANCERREFERALTYPE_DESC,

    ---Modality---
    ref_mod.FULL_DESCRIPTION AS CWT_MODALITY_DESC,

    --Patient Status--
    PATIENTSTATUS AS CWT_PATIENTSTATUS_CODE,

    --GP Fields
    cwt.ICS_OF_REGISTRATION AS REG_ICB_CODE,
    cwt.PDS_GPCODE AS REG_PRACTICE_CODE,
    --Residence Fields
    cwt.ICS_OF_RESIDENCE AS RES_ICB_CODE,
    cwt.LA_OF_RESIDENCE AS RES_LA_CODE,
    cwt.LSOA_OF_RESIDENCE AS RES_LSOA,
    --Pathway Identifiers
    cwt.PRIORITYTYPECODE AS PATHWAY_PRIORITYTYPE_CODE,
    cwt.CANCERTREATMENTEVENTTYPE AS PATHWAY_CANCERTREATMENTEVENTTYPE_CODE,
    cwt.SOURCEOFREFERRALFOROUTPATIENT AS PATHWAY_SOURCEOFREFERRALFOROUTPATIENT_CODE,
    cwt.CANCERTREATMENTMODALITY AS PATHWAY_CANCERTREATMENTMODALITY_CODE,
    cwt.CANCERCARESETTINGTREATMENT AS PATHWAY_CANCERCARESETTINGTREATMENT_CODE,
    cwt.FDPENDREASON AS PATHWAY_FDPENDREASON_CODE,
    cwt.FDPEXCLUSIONREASON AS PATHWAY_FDPEXCLUSIONREASON_CODE,
    cwt.FDPOUTCOMEMETHOD AS PATHWAY_FDPOUTCOMEMETHOD_CODE,
    cwt.FDPOUTCOMEPROFTYPE AS PATHWAY_FDPOUTCOMEPROFTYPE_CODE,
    --Date Fields
    cwt.DECTOREFDATE AS DATE_DECSIONTOREFERDATE,
    cwt.CRTPDATE AS DATE_CANCERREFERRALTOTREATMENTPERIODSTARTDATE, 
    cwt.CONSULTANTUPGRADEDATE AS DATE_CONSULTANTUPGRADEDATE, 
    cwt.DATEFIRSTSEEN AS DATE_DATEFIRSTSEEN,
    cwt.CANCERFASTERDIAGNOSISPATHWAYENDDATE AS DATE_FDSPATHWAYENDDATE,
    --cwt.MDTDATE AS DATE_MULTIDISCIPLINARYTEAMDISCUSSIONDATE,
    cwt.TRANSFERTOTREATMENTDATE AS DATE_TRANSFERTOTREATMENTDATE,
    cwt.CANCERTREATMENTPERIODSTARTDATE AS DATE_CANCERTREATMENTPERIODSTARTDATE,
    cwt.TREATMENTSTARTDATECANCER AS DATE_TREATMENTSTARTDATE,
    --Waiting Time Adjustments
    cwt.WAITINGTIMEADJUSTMENTFIRSTSEEN AS WTA_FIRSTSEENADJUSTMENT,
    cwt.WAITINGTIMEADJUSTMENTTREATMENT AS WTA_TREATMENTADJUSTMENT,
    cwt.WAITINGTIMEADJUSTMENTREASONTREATMENT AS WTA_TREATMENTREASON,
    --Geo
    cwt."dmIcbRegistrationSubmitted" = 'QMJ' AS GEO_GP,
    cwt."dmIcbResidenceSubmitted" = 'QMJ' AS GEO_RESIDENCE,
    COALESCE(
        cwt."Organisation_Code_CCG_of_DFS" = '93C' OR
        cwt."Organisation_Code_CCG_of_FDS" = '93C' OR
        cwt."Organisation_Code_CCG_of_TSD" = '93C',
        FALSE
    ) AS GEO_TRUST,
    org_fs.ORG_TRUST IN ('RAL', 'RAN', 'RAP', 'RKE', 'RRV', 'RP4', 'RP6') AS GEO_TRUST_DATEFIRSTSEEN,
    org_fdp.ORG_TRUST IN ('RAL', 'RAN', 'RAP', 'RKE', 'RRV', 'RP4', 'RP6') AS GEO_TRUST_FDS,
    org_at.ORG_TRUST IN ('RAL', 'RAN', 'RAP', 'RKE', 'RRV', 'RP4', 'RP6') AS GEO_TRUST_TREATMENTSTARTDATE,
    cwt.ACCOUNTABLEINVESTIGATINGPROVIDER IN ('RAL', 'RAN', 'RAP', 'RKE', 'RRV', 'RP4', 'RP6') AS GEO_TRUST_ACCOUNTABLEINVESTIGATING,
    org_cu.ORG_TRUST IN ('RAL', 'RAN', 'RAP', 'RKE', 'RRV', 'RP4', 'RP6') AS GEO_TRUST_CONSULTANTUPGRADE,
    --Event
    cwt.DATEFIRSTSEEN IS NOT NULL AS EVENT_DATEFIRSTSEEN,
    cwt.CANCERTREATMENTPERIODSTARTDATE IS NOT NULL AS EVENT_CANCERTREATMENTPERIOD,
    cwt.CANCERFASTERDIAGNOSISPATHWAYENDDATE IS NOT NULL AS EVENT_FDS,
    cwt.TREATMENTSTARTDATECANCER IS NOT NULL AS EVENT_TREATMENTSTARTDATE,
    --Metadata
    cwt."UniqSubmissionID" AS META_SUBMISSIONID

FROM DATA_LAKE.CWT."CWT001Data" cwt

--Organisation Site and Trust Mapping
---Consultant Upgrades---
LEFT JOIN org_par org_cu
ON org_cu.ORG_SITE = cwt.ORGCONSUPGRADE
---First Seen---
LEFT JOIN org_par org_fs
ON org_fs.ORG_SITE = cwt.ORGFIRSTSEEN
---Faster Diagnosis Standard---
LEFT JOIN org_par org_fdp
ON org_fdp.ORG_SITE = cwt.ORGFDPEND
---Pathway Identifier---
LEFT JOIN org_par org_pi
ON org_pi.ORG_SITE = cwt.ORGPPI
---Accountable Treating---
LEFT JOIN org_par org_at
ON org_at.ORG_SITE = cwt.ORGTREATSTART

--Reference Information Joins
---Primary Diagnosis---
LEFT JOIN DEV__MODELLING.CANCER__REF.DIM_CWT_REFERENCE ref_pd
ON cwt.PRIMARYDIAGNOSISICD = ref_pd.CODE
AND ref_pd.REFERENCE_CODE = 1

---Cancer Referral Type---
LEFT JOIN DEV__MODELLING.CANCER__REF.DIM_CWT_REFERENCE ref_rt
ON CAST(cwt.REFTYPE AS INT) = ref_rt.CODE
AND ref_rt.REFERENCE_CODE = 4

---Modality---
LEFT JOIN DEV__MODELLING.CANCER__REF.DIM_CWT_REFERENCE ref_mod
ON CAST(cwt.CANCERTREATMENTMODALITY AS INT) = ref_mod.CODE
AND ref_mod.REFERENCE_CODE = 20

--Clause to remove inactive records
WHERE EXISTS (
    SELECT NULL 
    FROM  DATA_LAKE.CWT."ActiveSystemId" asi 
    WHERE asi."dmicSystemId" = cwt."dmicSystemId")