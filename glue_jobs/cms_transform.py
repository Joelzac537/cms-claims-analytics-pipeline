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
    'Rndrng_NPI', 'Rndrng_Prvdr_Last_Org_Name', 'Rndrng_Prvdr_Type',
    'Rndrng_Prvdr_State_Abrvtn', 'Tot_Mdcr_Pymt_Amt', 'Tot_Sbmtd_Chrg',
    'Tot_Mdcr_Alowd_Amt', 'Tot_Benes', 'Tot_Srvcs',
    'Bene_CC_PH_Diabetes_V2_Pct', 'Bene_CC_PH_Hypertension_V2_Pct',
    'Bene_CC_PH_HF_NonIHD_V2_Pct', 'Bene_CC_PH_CKD_V2_Pct'
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
    when(col('Tot_Mdcr_Pymt_Amt') > 1033737, True).otherwise(False)
)

#--------Section 4------------#
## Write to S3 as Parquet partitioned by state
output_path = "s3://cms-medical-pipeline-jtz/processed/"

df.write\
    .mode("overwrite")\
    .partitionBy("Rndrng_Prvdr_State_Abrvtn")\
    .parquet(output_path)

## Commit job
job.commit()