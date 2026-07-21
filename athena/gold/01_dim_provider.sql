-- ============================================================
-- DIMENSION: provider
-- One row per provider (the grain of the source data).
-- Descriptive attributes only -- measures live in the fact view.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.dim_provider AS
SELECT DISTINCT
    rndrng_npi                 AS provider_npi,     -- natural key
    rndrng_prvdr_last_org_name AS provider_name,
    rndrng_prvdr_type          AS provider_type,
    rndrng_prvdr_state_abrvtn  AS state
FROM cms_medical_db.processed;
