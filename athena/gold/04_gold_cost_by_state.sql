-- ============================================================
-- GOLD MART: cost by state
-- Total / average Medicare payment and beneficiary counts per state.
-- Feeds the dashboard's "geography" view.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.gold_cost_by_state AS
SELECT
    state,
    COUNT(*)                        AS provider_count,
    ROUND(SUM(medicare_payment), 2) AS total_medicare_payment,
    ROUND(AVG(medicare_payment), 2) AS avg_medicare_payment,
    SUM(beneficiaries)              AS total_beneficiaries
FROM cms_medical_db.fact_provider_metrics
GROUP BY state
ORDER BY total_medicare_payment DESC;
