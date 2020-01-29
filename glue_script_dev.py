import sys,os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

STAGE = 'dev'
SERVICE = 'userengg'

db_name = STAGE + '-' + SERVICE + '-db'
table_name = STAGE + '-' + SERVICE.replace('-','_') + '-dynamotable'
path_name =  "s3://" + STAGE + '-' + SERVICE + '-glue-bucket'

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = table_name, transformation_ctx = "datasource0")
datasink2 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": path_name}, format = "csv", transformation_ctx = "datasink2")
job.commit()