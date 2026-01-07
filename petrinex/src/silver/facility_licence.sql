CREATE OR REPLACE MATERIALIZED VIEW facility_licence AS

SELECT 
LicenceType AS `licence_type`,
LicenceNumber AS `licence_number`,
LicenceStatus AS `licence_status`,
CAST(LicenceStatusDate AS DATE) AS `licence_status_date`,
Licensee AS `licensee`,
LicenseeName AS `licensee_name`,
EnergyDevelopmentCategoryType AS `energy_development_category_type`,
LicenceLocation AS `licence_location`,
LicenceLegalSubdivision AS `licence_legal_subdivision`,
LicenceSection AS `licence_section`,
LicenceTownship AS `licence_township`,
LicenceRange AS `licence_range`,
LicenceMeridian AS `licence_meridian`,
OrphanWellFlg AS `orphan_well_flag`,
"AB" AS `licence_province`
FROM ${source_catalog}.petrinex.facility_licence_ab

UNION

SELECT 
LicenceType AS `licence_type`,
LicenceNumber AS `licence_number`,
LicenceStatus AS `licence_status`,
CAST(LicenceStatusDate AS DATE) AS `licence_status_date`,
Licensee AS `licensee`,
LicenseeName AS `licensee_name`,
EnergyDevelopmentCategoryType AS `energy_development_category_type`,
LicenceLocation AS `licence_location`,
LicenceLegalSubdivision AS `licence_legal_subdivision`,
LicenceSection AS `licence_section`,
LicenceTownship AS `licence_township`,
LicenceRange AS `licence_range`,
LicenceMeridian AS `licence_meridian`,
OrphanWellFlg AS `orphan_well_flag`,
"SK" AS `licence_province`
FROM ${source_catalog}.petrinex.facility_licence_sk;