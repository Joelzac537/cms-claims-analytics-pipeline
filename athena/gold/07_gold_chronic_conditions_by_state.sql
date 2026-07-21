-- ============================================================
-- GOLD MART: chronic conditions by state
-- Average prevalence of four chronic conditions across each state's
-- provider panels. Feeds the dashboard's "clinical" view.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.gold_chronic_conditions_by_state AS
SELECT
    state,
    ROUND(AVG(pct_diabetes), 2)      AS avg_pct_diabetes,
    ROUND(AVG(pct_hypertension), 2)  AS avg_pct_hypertension,
    ROUND(AVG(pct_heart_failure), 2) AS avg_pct_heart_failure,
    ROUND(AVG(pct_ckd), 2)           AS avg_pct_ckd
FROM cms_medical_db.fact_provider_metrics
GROUP BY state
ORDER BY state;
