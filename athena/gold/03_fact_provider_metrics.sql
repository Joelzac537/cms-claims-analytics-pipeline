-- ============================================================
-- FACT: provider metrics
-- The measures for each provider, with friendly column names and
-- foreign keys (provider_npi, provider_type, state) into the dimensions.
-- ============================================================
CREATE OR REPLACE VIEW cms_medical_db.fact_provider_metrics AS
SELECT
    -- keys
    rndrng_npi                     AS provider_npi,
    rndrng_prvdr_type              AS provider_type,
    rndrng_prvdr_state_abrvtn      AS state,

    -- cost / volume measures
    tot_mdcr_pymt_amt              AS medicare_payment,
    tot_sbmtd_chrg                 AS submitted_charge,
    tot_mdcr_alowd_amt             AS medicare_allowed,
    tot_benes                      AS beneficiaries,
    tot_srvcs                      AS services,
    is_high_cost_outlier           AS is_high_cost_outlier,

    -- chronic-condition prevalence (% of a provider's beneficiaries)
    bene_cc_ph_diabetes_v2_pct     AS pct_diabetes,
    bene_cc_ph_hypertension_v2_pct AS pct_hypertension,
    bene_cc_ph_hf_nonihd_v2_pct    AS pct_heart_failure,
    bene_cc_ph_ckd_v2_pct          AS pct_ckd
FROM cms_medical_db.processed;
