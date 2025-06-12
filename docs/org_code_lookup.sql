--Script to map NCL Site Codes to Provider Codes
SELECT 
	child.[Organisation_Code] AS site_code, 
	parent.[Organisation_Code] AS prov_code

FROM [Dictionary].[dbo].[Organisation] child

LEFT JOIN [Dictionary].[dbo].[Organisation] parent
ON parent.SK_OrganisationID = child.SK_OrganisationID_ParentOrg

--Filter to only sites (have a parent)
WHERE child.SK_OrganisationID_ParentOrg IS NOT NULL
--Filter to NCL or RNOH (RAN) which is mislabelled as NWL 
AND (
	(parent.SK_OrganisationID_ParentOrg = '440028'
	AND parent.SK_OrganisationTypeID = 41)
	OR parent.Organisation_Code = 'RAN'
	)
--Filter out CNWL which is mislabelled as NCL
AND parent.Organisation_Code != 'RV3'
