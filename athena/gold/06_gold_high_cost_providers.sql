-- ============================================================
-- GOLD MART: high-cost providers
-- The providers flagged as high-cost outliers by the ETL job,
-- ranked by Medicare payment. Also the label source for the ML model.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.gold_high_cost_providers AS
SELECT
    provider_npi,
    provider_type,
    state,
    medicare_payment,
    beneficiaries,
    services
FROM cms_medical_db.fact_provider_metrics
WHERE is_high_cost_outlier = true
ORDER BY medicare_payment DESC;
