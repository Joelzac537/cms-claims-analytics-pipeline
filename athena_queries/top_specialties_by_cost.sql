SELECT 
    rndrng_prvdr_type,
    COUNT(*) as provider_count,
    ROUND(AVG(tot_mdcr_pymt_amt), 2) as avg_payment,
    ROUND(SUM(tot_mdcr_pymt_amt), 2) as total_payment
FROM "cms_medical_db"."processed"
GROUP BY rndrng_prvdr_type
ORDER BY avg_payment DESC
LIMIT 10;