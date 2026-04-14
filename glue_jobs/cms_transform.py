import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

#--------Section 4------------#
## Write to S3 as Parquet partitioned by state
output_path = "s3://cms-medical-pipeline-jtz/processed/"

df.write\
    .mode("overwrite")\
    .partitionBy("rndrng_prvdr_state_abrvtn")\
    .parquet(output_path)

## Commit job
job.commit()