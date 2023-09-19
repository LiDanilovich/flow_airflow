from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
import sys, os
import json
import boto3
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2

print("STARTING ...")
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'candidate_limit_n', 'traditional_limit_n', 'ctrl_grp_limit_perc'])
job.init(args['JOB_NAME'], args)

today = (datetime.today()-timedelta(days=0)).strftime('%Y%m%d')
year=today[0:4]
month=str(today)[4:6]
day=str(today)[6:8]

def ingest_input_offerfit_function(year, month, day):

	#Creates a list of files
	s3_resource = boto3.resource('s3')
	publibucket = s3_resource.Bucket('aiq-bucket-exchange')
	objects = publibucket.objects.filter(Prefix = 'offers/')
	thepath = "s3://cdp-lcpr-process/offers/"
	objects_to_read = [thepath + o.key.split('/')[-1] for o in objects if o.key.endswith('.csv')]
	expected = year+month+day
	files = [x for x in objects_to_read if expected in x]
	print("expected: ", expected)
	print("The files are :", files)

	if len(files) > 0:
		input_offerfit_file = spark.read.option("header","true").option("delimiter", ",").csv(files)
		input_offerfit_file.createOrReplaceTempView("input_offerfit_file")
		print("The original df has ", str(df.count()), " rows ")

		input_offerfit_df = spark.sql(f"""
		select 
		cast(SUB_ACCT_NO as bigint) as SUB_ACCT_NO,
        cast(OLD_SPEED as string) as OLD_SPEED,	  
		cast(FROM_CSG_CODE as bigint) as FROM_CSG_CODE,
		cast(TO_CSG_CODE as string) as TO_CSG_CODE,
		cast(NEW_SPEED as string) as NEW_SPEED,
		cast(PAYMENT_DIF as bigint) as PAYMENT_DIF,
		cast(DISCOUNT as bigint) as DISCOUNT,	  
		cast(SUBLINE_1 as string) as SUBLINE_1,
		cast(CALL_TO_ACTION as string) as CALL_TO_ACTION,  
		cast(SUBJECT_CODE as string) as SUBJECT_CODE,
		cast(TEMPLATE_TYPE as bigint) as TEMPLATE_TYPE,
		cast(TIME_FRAME as string) as TIME_FRAME,
		cast(REGIME as string) as REGIME,
		cast(DATE as string) as DATE,
		cast(STB as string) as STB
		from input_offerfit_file
		""")

ingest_input_offerfit_function(year, month, day)

job.commit()