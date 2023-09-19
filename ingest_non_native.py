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
import time
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

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

def extract_from_mask(filename, channel):
    mask_file = ''
    Campaign_desc = ''
    
    index = filename.find('_export')
    if index != -1:
        mask_file = filename[:index+1]
        print(mask_file)
    
    index = filename.find(f'_{channel}')
    if index != -1:
        Campaign_desc = filename[5:index]
        print(Campaign_desc)
        
    return mask_file, Campaign_desc

def check_status_crawler(crawler_name):
    response = glue_client.get_crawler(Name=crawler_name)
    crawler_state = response['Crawler']['State']
    
    # Verifica si el Crawler ha terminado
    if crawler_state == 'STOPPING' or crawler_state == 'READY':
        return True
    else:
        return False

def ingest_emarsys_cc_function(prefix, key, bucket):
    
    #Creates a list of files
    s3_resource = boto3.resource('s3')
    objects_to_read = str(key)
    theFile = 's3://' + str(bucket) + '/' + str(prefix) + str(objects_to_read)
    print('theFile: '+ str(theFile))
    
    # Crea una sesión de Spark
    spark = SparkSession.builder.appName("LoadCSV").getOrCreate()
    input_emarsys_cc_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(theFile)
    
    # Limpia los nombres de las columnas reemplazando espacios en blanco con underscores
    
    cleaned_header = [col.replace("(", "_").replace(")", "_") for col in input_emarsys_cc_df.columns]
    cleaned_column_names = [re.sub(r'\s+', '_', col) for col in cleaned_header]
    input_emarsys_cc_df = input_emarsys_cc_df.toDF(*cleaned_column_names)
    
    input_emarsys_cc_df.createOrReplaceTempView("input_emarsys_cc_df")
    
    if input_emarsys_cc_df.count() < 1:
        print(f'El archivo {objects_to_read} para CC esta vacio')
        raise Exception(f'El archivo {objects_to_read} para CC esta vacio')
    
    mask_file, campaign_name = extract_from_mask(objects_to_read,'CALLCENTER')
    
    input_emarsys_cc = spark.sql(f"""
    select t1.*,
    'CALLCENTER' as channel,
    '{campaign_name}' as campaign_name,
    '{str(today_year)+str(today_month)}' as month,
    '{str(today_year)+str(today_month)+str(today_day)}' as day,
    '{str(today_year)+str(today_month)+str(today_day)+str(hour_min_sec)}' as day_hour
    from input_emarsys_cc_df t1
    """)
    
    print("The input_emarsys_cc has ", str(input_emarsys_cc.count()), " rows ")
    s3path = "s3://cdp-lcpr-process/hist/hist_emarsys_cc_in"
    input_emarsys_cc = input_emarsys_cc.repartition("month", "day", "day_hour")
    
    input_emarsys_cc \
    .write.mode('overwrite') \
    .format('parquet') \
    .partitionBy('month', 'day', 'day_hour') \
    .save( s3path )
    
    create_partition_v2('db_dev_cdp_project', 'hist_emarsys_cc_in', str(today_year)+str(today_month)+str(today_day)+str(hour_min_sec))
    
    input_emarsys_cc.show(10, truncate=False)
    
    s3path2 = "s3://cdp-lcpr-process/cc/csv_out"
    
    input_emarsys_cc = input_emarsys_cc.drop(col("month")).drop(col("day")).drop(col("day_hour")).drop(col("channel")).drop(col("campaign_name")).repartition(1)
    
    input_emarsys_cc \
    .write.mode('overwrite') \
    .format('csv') \
    .option("header","true") \
    .option("ignoreTrailingWhiteSpace","true") \
    .option("ignoreLeadingWhiteSpace","true") \
    .save( s3path2 )
    
    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = 'cc/csv_out/')
    objects_to_read = [o.key.split('/')[-1] for o in objects if o.key.endswith('.csv') and 'part' in o.key][0]
    theobject= str(bucket)+'/cc/csv_out/'+objects_to_read
    thekey= 'cc/csv_out/'+ objects_to_read
    writtenobject = 'cc/csv_out/'+ mask_file+today_year+today_month+today_day+hour_min_sec+".csv"
    
    s3_resource.Object('cdp-lcpr-process', writtenobject).copy_from(CopySource= theobject)
    s3_resource.Object('cdp-lcpr-process',thekey).delete()
    
    source_file = f'cdp-lcpr-process/{writtenobject}'
    file_key = 'in/'+ mask_file+today_year+today_month+today_day+hour_min_sec+".csv"
    
    s3_resource.Object('cdp-lcpr-transfer-call-center', file_key).copy_from(CopySource= source_file)
    
    query = spark.sql("""DROP TABLE db_dev_cdp_project.hist_emarsys_cc_in""")
    
    # Ejecuta el Crawler
    crawler_name = 'craw_hist_emarsys_cc'
    response = glue_client.start_crawler(Name=crawler_name)
    print("Crawler execution started: ", response)
    
    while check_status_crawler(crawler_name) == False:
        time.sleep(15)
    
    print('Actualiza: db_dev_cdp_project.hist_emarsys_cc_in')



    
if method == 'emarsys_cc_in':
    ingest_emarsys_cc_function(prefix, key, bucket)
    
    
job.commit()
