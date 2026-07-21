import sys
from awsglue.transforms import *
from awsglue.transforms import SelectFromCollection
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import col, when

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Read from Glue Data Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="cms_medical_db",
    table_name="raw"
)

#--------Section 2-------------#
## Convert to Spark dataframe and select columns
df = datasource.toDF()

columns_needed = [
    'rndrng_npi', 'rndrng_prvdr_last_org_name', 'rndrng_prvdr_type',
    'rndrng_prvdr_state_abrvtn', 'tot_mdcr_pymt_amt', 'tot_sbmtd_chrg',
    'tot_mdcr_alowd_amt', 'tot_benes', 'tot_srvcs',
    'bene_cc_ph_diabetes_v2_pct', 'bene_cc_ph_hypertension_v2_pct',
    'bene_cc_ph_hf_nonihd_v2_pct', 'bene_cc_ph_ckd_v2_pct'
]

df = df.select([col(c) for c in columns_needed])

#--------Section 3------------#
## Fill null values with 0 for chronic condition columns
chronic_cols = [
    'Bene_CC_PH_Diabetes_V2_Pct', 'Bene_CC_PH_Hypertension_V2_Pct',
    'Bene_CC_PH_HF_NonIHD_V2_Pct', 'Bene_CC_PH_CKD_V2_Pct'
]

for c in chronic_cols:
    df = df.fillna({c: 0})

## Fix Tot_Srvcs to integer
df = df.withColumn('Tot_Srvcs', col('Tot_Srvcs').cast('int'))

## Add outlier flag
df = df.withColumn('is_high_cost_outlier',
    when(col('tot_mdcr_pymt_amt') > 1033737, True).otherwise(False)
)

#--------Section 4: Data Quality (warn-only)------------#
## Evaluate data quality on the transformed data before writing.
## Warn-only: results are published to S3 + CloudWatch but do NOT stop the job.
## Cache so the DQ pass and the Parquet write don't recompute the transforms twice.
df.cache()

dq_ruleset = """
Rules = [
    RowCount > 1000000,
    IsComplete "rndrng_npi",
    Uniqueness "rndrng_npi" > 0.95,
    IsComplete "rndrng_prvdr_state_abrvtn",
    ColumnLength "rndrng_prvdr_state_abrvtn" = 2,
    IsComplete "rndrng_prvdr_type",
    ColumnValues "tot_mdcr_pymt_amt" >= 0,
    ColumnValues "tot_benes" >= 0,
    ColumnValues "tot_srvcs" >= 0,
    -- NOTE: DQDL "between" is EXCLUSIVE of its bounds, and these percentages
    -- legitimately include 0 (nulls are filled with 0 in the ETL). We widen
    -- to -1..101 so the valid 0-100 range passes; a truly bad value
    -- (negative or >100) still fails.
    ColumnValues "bene_cc_ph_diabetes_v2_pct" between -1 and 101,
    ColumnValues "bene_cc_ph_hypertension_v2_pct" between -1 and 101,
    ColumnValues "bene_cc_ph_hf_nonihd_v2_pct" between -1 and 101,
    ColumnValues "bene_cc_ph_ckd_v2_pct" between -1 and 101
]
"""

dq_input = DynamicFrame.fromDF(df, glueContext, "dq_input")

dq_results = EvaluateDataQuality().process_rows(
    frame=dq_input,
    ruleset=dq_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "cms_etl_dq",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"}
)

## Pull the per-rule pass/fail outcomes and write them to S3.
## The write is also the action that FORCES the DQ evaluation to run --
## Spark is lazy, so ignoring this collection would skip DQ entirely.
rule_outcomes = SelectFromCollection.apply(dfc=dq_results, key="ruleOutcomes")
rule_outcomes.toDF().coalesce(1).write.mode("overwrite").json(
    "s3://cms-medical-pipeline-jtz/dq-results/cms_etl_dq/"
)

#--------Section 5------------#
## Write to S3 as Parquet partitioned by state
output_path = "s3://cms-medical-pipeline-jtz/processed/"

## repartition by the partition key -> ~1 file per state instead of thousands
## of tiny files (cheaper/faster Athena scans in the gold layer later).
df.repartition("rndrng_prvdr_state_abrvtn")\
    .write\
    .mode("overwrite")\
    .partitionBy("rndrng_prvdr_state_abrvtn")\
    .parquet(output_path)

## Commit job
job.commit()