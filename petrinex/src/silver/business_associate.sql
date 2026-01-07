CREATE OR REPLACE MATERIALIZED VIEW business_associate AS

SELECT
  BAIdentifier AS `ba_id`,
  BALegalName AS `ba_legal_name`,
  BAAddress AS `ba_address`,
  BAPhoneNumber AS `ba_phone_number`,
  BACorporateStatus AS `ba_corporate_status`,
  CAST(BACorporateStatusEffectiveDate AS DATE) AS `ba_corporate_status_effective_date`,
  AmalgamatedIntoBAID AS `amalgamated_into_ba_id`,
  AmalgamatedIntoBALegalName AS `amalgamated_into_ba_legal_name`,
  CAST(BAAmalgamationEstablishedDate AS DATE) AS `ba_amalgamation_established_date`,
  BALicenceEligibilityType AS `ba_licence_eligibility_type`,
  BALicenceEligibiltyDesc AS `ba_licence_eligibility_description`,
  BAAbbreviatedName AS `ba_abbreviated_name`,
  "AB" AS `ba_province`
FROM
  ${source_catalog}.petrinex.business_associate_ab

UNION

SELECT
  BAIdentifier AS `ba_id`,
  BALegalName AS `ba_legal_name`,
  BAAddress AS `ba_address`,
  BAPhoneNumber AS `ba_phone_number`,
  BACorporateStatus AS `ba_corporate_status`,
  CAST(BACorporateStatusEffectiveDate AS DATE) AS `ba_corporate_status_effective_date`,
  AmalgamatedIntoBAID AS `amalgamated_into_ba_id`,
  AmalgamatedIntoBALegalName AS `amalgamated_into_ba_legal_name`,
  CAST(BAAmalgamationEstablishedDate AS DATE) AS `ba_amalgamation_established_date`,
  BALicenceEligibilityType AS `ba_licence_eligibility_type`,
  BALicenceEligibiltyDesc AS `ba_licence_eligibility_description`,
  BAAbbreviatedName AS `ba_abbreviated_name`,
  "SK" AS `ba_province`
FROM
  ${source_catalog}.petrinex.business_associate_sk
;