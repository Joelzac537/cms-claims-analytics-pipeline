select rndrng_prvdr_state_abrvtn, count(*) as total_provider, 
ROUND(AVG(tot_mdcr_pymt_amt), 2) as avg_payment, 
ROUND(SUM(tot_mdcr_pymt_amt), 2) as total_payment FROM "cms_medical_db"."processed" 
GROUP BY rndrng_prvdr_state_abrvtn 
order by total_payment desc 
limit 10;