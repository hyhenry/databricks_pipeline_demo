CREATE OR REPLACE MATERIALIZED VIEW well_licence AS

SELECT 
LicenceType AS `licence_type`,
REGEXP_REPLACE(LicenceNumber, '^0*', '') as `licence_number`,
CAST(LicenceIssueDate AS DATE) AS `licence_issue_date`,
LicenceStatus AS `licence_status`,
CAST(LicenceStatusDate AS DATE) AS `licence_status_date`,
Licensee AS `licensee`,
LicenseeName AS `licensee_name`,
LicenceLocation AS `licence_location`,
LicenceLegalSubdivision AS `licence_legal_subdivision`,
LicenceSection AS `licence_section`,
LicenceTownship AS `licence_township`,
LicenceRange AS `licence_range`,
LicenceMeridian AS `licence_meridian`,
DrillingOperationType AS `drilling_operation_type`,
WellPurpose AS `well_purpose`,
WellLicenceType AS `well_licence_type`,
WellSubstance AS `well_substance`,
ProjectedFormation AS `projected_formation`,
TerminatingFormation AS `terminating_formation`,
CAST(ProjectedTotalDepth AS DECIMAL(10,4)) AS `projected_total_depth`,
AERClass AS `aer_class`,
HeadLessor AS `head_lessor`,
WellCompletionType AS `well_completion_type`,
TargetPool AS `target_pool`,
OrphanWellFlg AS `orphan_well_flag`,
"AB" AS `licence_province`
FROM ${source_catalog}.petrinex.well_licence_ab

UNION

SELECT 
LicenceType AS `licence_type`,
REGEXP_REPLACE(LicenceNumber, '^0*', '') as `licence_number`,
CAST(LicenceIssueDate AS DATE) AS `licence_issue_date`,
LicenceStatus AS `licence_status`,
CAST(LicenceStatusDate AS DATE) AS `licence_status_date`,
Licensee AS `licensee`,
LicenseeName AS `licensee_name`,
LicenceLocation AS `licence_location`,
LicenceLegalSubdivision AS `licence_legal_subdivision`,
LicenceSection AS `licence_section`,
LicenceTownship AS `licence_township`,
LicenceRange AS `licence_range`,
LicenceMeridian AS `licence_meridian`,
DrillingOperationType AS `drilling_operation_type`,
WellPurpose AS `well_purpose`,
WellLicenceType AS `well_licence_type`,
WellSubstance AS `well_substance`,
ProjectedFormation AS `projected_formation`,
TerminatingFormation AS `terminating_formation`,
CAST(ProjectedTotalDepth AS DECIMAL(10,4)) AS `projected_total_depth`,
AERClass AS `aer_class`,
HeadLessor AS `head_lessor`,
WellCompletionType AS `well_completion_type`,
TargetPool AS `target_pool`,
OrphanWellFlg AS `orphan_well_flag`,
"SK" AS `licence_province`
FROM ${source_catalog}.petrinex.well_licence_sk
;