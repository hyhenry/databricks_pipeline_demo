CREATE OR REPLACE MATERIALIZED VIEW facility_operator_history AS

SELECT 
FacilityID AS `facility_id`,
FacilityType AS `facility_type`,
FacilityIdentifier AS `facility_identifier`,
FacilityName AS `facility_name`,
FacilitySubType AS `facility_sub_type`,
FacilitySubTypeDesc AS `facility_sub_type_description`,
OperatorBAID AS `operator_ba_id`,
OperatorName AS `operator_name`,
StartDate AS `start_date`,
EndDate AS `end_date`,
FacilityProvinceState AS `facility_province`
FROM ${source_catalog}.petrinex.facility_operator_history_ab

UNION

SELECT 
FacilityID AS `facility_id`,
FacilityType AS `facility_type`,
FacilityIdentifier AS `facility_identifier`,
FacilityName AS `facility_name`,
FacilitySubType AS `facility_sub_type`,
FacilitySubTypeDesc AS `facility_sub_type_description`,
OperatorBAID AS `operator_ba_id`,
OperatorName AS `operator_name`,
StartDate AS `start_date`,
EndDate AS `end_date`,
FacilityProvinceState AS `facility_province`
FROM ${source_catalog}.petrinex.facility_operator_history_sk
;