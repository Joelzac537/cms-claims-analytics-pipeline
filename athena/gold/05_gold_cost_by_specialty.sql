-- ============================================================
-- GOLD MART: cost by specialty
-- Which provider types drive the highest average Medicare payment.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.gold_cost_by_specialty AS
SELECT
    provider_type,
    COUNT(*)                        AS provider_count,
    ROUND(AVG(medicare_payment), 2) AS avg_medicare_payment,
    ROUND(SUM(medicare_payment), 2) AS total_medicare_payment
FROM cms_medical_db.fact_provider_metrics
GROUP BY provider_type
ORDER BY avg_medicare_payment DESC;
