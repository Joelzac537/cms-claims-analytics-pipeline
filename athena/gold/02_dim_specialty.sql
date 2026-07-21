-- ============================================================
-- DIMENSION: specialty (provider type)
-- Distinct specialties with how many providers fall under each.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.dim_specialty AS
SELECT
    rndrng_prvdr_type AS provider_type,
    COUNT(*)          AS provider_count
FROM cms_medical_db.processed
GROUP BY rndrng_prvdr_type;
