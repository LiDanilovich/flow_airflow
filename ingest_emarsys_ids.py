import sys, re
import boto3
from datetime import date, timedelta, datetime
import json
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2

print("STARTING ...")
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME","bucket","prefix","key"])

bucket = args['bucket']
prefix = args['prefix']
key = args['key']

file = f's3://{bucket}/{prefix}/{key}'
print(file)

'''
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

file = args["key"]
print(file)
'''

today = (datetime.today()-timedelta(days=0)).strftime('%Y%m%d')
year=today[0:4]
month=str(today)[4:6]
day=str(today)[6:8]


#backup the csv inputs 
#Copia el input de s3://s3-01-cdp-exchange-lcpr/home/emarsys/out/LCPR_All_Contacts_export_  a s3://cdp-lcpr-process/emarsys_id/out_backup

def move_to_bkp():
     s3_resource = boto3.resource('s3')
     publibucket = s3_resource.Bucket('s3-01-cdp-exchange-lcpr')
     objects = publibucket.objects.filter(Prefix = 'home/emarsys/out/')
     
     thepath = "s3://s3-01-cdp-exchange-lcpr/home/emarsys/out/"
     objects_to_copy = [thepath + o.key.split('/')[-1] for o in objects if o.key.endswith('.csv')]

     expected = 'LCPR_Fixed_All_Contacts_export_'
     files = [x for x in objects_to_copy if expected in x]
     print("The objects to move are : ", files)
     files_to_delete = []
     for key in files:
         target = "emarsys_id/out_backup/"+key.split('/')[-1]
         print(" target value is:", target)
         thekey =  "home/emarsys/out/"+key.split('/')[-1]
         files_to_delete.append({"Key": thekey})
         copy_source = {
          'Bucket': str('s3-01-cdp-exchange-lcpr'),
          'Key': thekey
         }
         print("Copy source is :", copy_source)
         bucket = s3_resource.Bucket('cdp-lcpr-process')
         bucket.copy(copy_source, str(target))     
         
     print("Files a eliminar: ",files_to_delete)
     s3_resource.meta.client.delete_objects(Bucket='s3-01-cdp-exchange-lcpr', Delete={'Objects': files_to_delete})



#Creates a list of files in s3://s3-01-cdp-exchange-lcpr/home/emarsys/out/LCPR_Fixed_All_Contacts_export_ that matches with the current date
'''
s3_resource = boto3.resource('s3')
publibucket = s3_resource.Bucket('s3-01-cdp-exchange-lcpr')
objects = publibucket.objects.filter(Prefix = 'home/emarsys/out/')
thepath = "s3://s3-01-cdp-exchange-lcpr/home/emarsys/out/"
objects_to_read = [thepath + o.key.split('/')[-1] for o in objects if o.key.endswith('.csv')]
expected = "LCPR_Fixed_All_Contacts_export_"
files = [x for x in objects_to_read if expected in x]
print("The files are :", files)
print("The expected is :", expected)
'''


if bool(file):
    
	df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(file)
	cleaned_header = [col.replace("(", "_").replace(")", "_") for col in df.columns]
	cleaned_column_names = [re.sub(r'\s+', '_', col) for col in cleaned_header]
	df = df.toDF(*cleaned_column_names)
	df.printSchema()
	
	df.createOrReplaceTempView("df")
	
	formatteddf = spark.sql(f""" 
	select 
	cast(user_id as bigint) as user_id,
	cast(Lcpr_AIQ_Id as string) as lcpr_aiq_id,
	'{str(year)+str(month)+str(day)}' as process_day
	from df
	where Lcpr_AIQ_Id like 'LCPR_FX_%'
	""")
	
	formatteddf.printSchema()
	
	formatteddf.show(3, truncate=False)
	formatteddf.createOrReplaceTempView("formatteddf")
	
	s3path = "s3://cdp-lcpr-process/emarsys_id/emarsys_id/"
	
	formatteddf \
	.write.mode('overwrite') \
	.format('parquet') \
	.save( s3path )
	
	# resguardo 
	# move_to_bkp()
else:
    print('No existe archivo a procesar')
    
job.commit()