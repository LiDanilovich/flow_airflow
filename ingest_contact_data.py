from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from datetime import date, timedelta, datetime
import sys, os, re
import json
import boto3
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2

print("STARTING ...")
## @params: [JOB_NAME]
## @params: [JOB_NAME]
glue_client = boto3.client('glue', region_name='us-east-1')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'prefix', 'key', 'method'])
job.init(args['JOB_NAME'], args)

bucket = args['bucket']
prefix = args['prefix']
key = args['key']
method = args['method']

today = (datetime.today()-timedelta(days=0)).strftime('%Y%m%d %H%M%S')
today_year=today[0:4]
today_month=str(today)[4:6]
today_day=str(today)[6:8]
hour_min_sec = str(today)[9:15]

def extract_date_from_mask(mask):
    match = re.search(r'(\d{4})\D+(\d{2})\D+(\d{2})', mask)
    
    if match:
        year, month, day = match.groups()
        return year, month, day
    else:
        return None, None, None
    
def extract_date_parts(key):
    match = re.search(r'(\d{4})(\d{2})(\d{2})', key)
    
    if match:
        year, month, day = match.groups()
        return year, month, day
    else:
        return None, None, None

def ingest_emarsys_contact_data_function(prefix, key, bucket):

	#Creates a list of files
	s3_resource = boto3.resource('s3')
	publibucket = s3_resource.Bucket(str(bucket))
	objects = publibucket.objects.filter(Prefix = str(prefix))
	objects_to_read = str(key)
	year, month, day = extract_date_from_mask(key)
	theFile = 's3://' + str(bucket) + '/' + str(prefix) + str(objects_to_read)
	print('publibucket: '+ str(publibucket))
	print('objects: '+ str(objects))
	print('objects_to_read: '+ str(objects_to_read))
	print('fecha archivo: '+ str(year)+str(month)+str(day))
	print('theFile: '+ str(theFile))
	
	
	# Crea una sesión de Spark
	spark = SparkSession.builder.appName("LoadCSV").getOrCreate()
	
	input_emarsys_contact_data_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(theFile)
	
	# Limpia los nombres de las columnas reemplazando espacios en blanco con underscores
	cleaned_header = [col.replace("(", "_").replace(")", "_") for col in input_emarsys_contact_data_df.columns]
	cleaned_column_names = [re.sub(r'\s+', '_', col) for col in cleaned_header]
	input_emarsys_contact_data_df = input_emarsys_contact_data_df.toDF(*cleaned_column_names)
	
	# Crea un DataFrame temporal
	input_emarsys_contact_data_df.createOrReplaceTempView("input_emarsys_contact_data")
	
	input_emarsys_contact_data = spark.sql(f"""
	select t1.*,
	'{str(today_year)+str(today_month)}' as month,
	'{str(today_year)+str(today_month)+str(today_day)}' as day,
	'{str(today_year)+str(today_month)+str(today_day)+str(hour_min_sec)}' as day_hour
	from input_emarsys_contact_data t1
	""")
	
	print("The input_offerfit_fin_df has ", str(input_emarsys_contact_data.count()), " rows ")
	s3path = "s3://cdp-lcpr-process/hist/emarsys_contact_data"
	input_emarsys_contact_data = input_emarsys_contact_data.repartition("month", "day", "day_hour")
	
	input_emarsys_contact_data \
			.write.mode('overwrite') \
			.format('parquet') \
			.partitionBy('month', 'day', 'day_hour') \
			.save( s3path )
	
	create_partition_v2('db_dev_cdp_project', 'emarsys_contact_data', str(year)+str(month)+str(day)+str(hour_min_sec))

	# Ejecuta el Crawler
	crawler_name = 'craw_emarsys_contact_data'    
	response = glue_client.start_crawler(Name=crawler_name)
	print("Crawler execution started: ", response)

if method == 'emarsys_contact_data_in':
	ingest_emarsys_contact_data_function(prefix, key, bucket)


job.commit()
