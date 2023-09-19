from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
from pyspark.sql import functions as F
import sys, os
import json
import boto3
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2



print("STARTING ...")
## @params: [JOB_NAME]


today = (datetime.today()-timedelta(days=0)).strftime('%Y%m%d %H%M%S')
year=today[0:4]
month=str(today)[4:6]
day=str(today)[6:8]
hour_min_sec = str(today)[9:15]

args = getResolvedOptions(sys.argv, ['bucket','prefix','key'])
bucket = args['bucket']
prefix = args['prefix']
key = args['key']

# bucket = 'cdp-lcpr-offers-bucket'
# prefix = 'Offerfit/in/'
# key = 'lcpr_offers_offerfit_20230808.csv'

print("the source is: ",bucket,prefix,key)

# Create a GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Get the table name from command line arguments (use your table name here)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME'])
database_name = args['DATABASE_NAME']
table_name = args['TABLE_NAME']

dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="dyf"
)

df = dyf.toDF()
df.printSchema()

if df.count() > 0:
    max_id_oferta_int_value = df.select(F.max("id_oferta_int")).collect()[0][0]
    max_id_oferta_int = max_id_oferta_int_value if max_id_oferta_int_value is not None else 0
else:
    max_id_oferta_int = 0
print (max_id_oferta_int)

theFile = 's3://' + bucket + '/' + prefix + key
df = spark.read.option("header","true").option("delimiter", ",").csv(theFile)
df.createOrReplaceTempView("df")

formatteddf = spark.sql(f""" with formatteddf as 
	(select t1.*, row_number() over(order by sub_acct_no asc) + """+str(max_id_oferta_int)+""" as id_oferta_int 
	from df t1)  
   select 
   sub_acct_no,
   pkg_cde,
   online_descr_pkg,
   smt_descr_pkg,
   hsd_service,
   cast(bundlecharge_csg as double) as bundlecharge_csg,	
   recommendedpkg,
   tocsgcodefriendlyname,	
   to_online_descr_pkg,
   to_smt_descr_pckg,
   to_bundlecharge_csg,
   discount,
   date,
   rank,	
   pkg_type,
   stb,
   cast(additional_charge as double) as additional_charge,	
   delta_arpu,
   source, 	
   regime,
   cast(reward as double) as reward,	
   use_case,	
   offer_type,	
   channel,	
   week_day	,
   cast(next_best_action_date as date) as next_best_action_date,	
   time_frame, 
   additional_param_1,	
   additional_param_2,
   additional_param_3,
   additional_param_4,	
   additional_param_5,
   additional_param_6,	
   additional_param_7,	
   additional_param_8,	
   additional_param_9,	
   additional_param_10	,
   id_oferta_int, """ +
   str(year)+str(month) + """ as month, """+
   str(year)+str(month)+str(day) +""" as day,""" +
   str(year)+str(month)+str(day)+str(hour_min_sec)+""" as day_hour
   from formatteddf  """)
   
formatteddf.printSchema()
formatteddf.show(3, truncate=False)

formatteddf.createOrReplaceTempView("formatteddf")

s3path = "s3://cdp-lcpr-process/offers/in/"
    
if formatteddf.count() > 0:
  formatteddf = formatteddf.repartition("month", "day", "day_hour")

  formatteddf \
      .write.mode('overwrite') \
      .format('parquet') \
      .partitionBy('month', 'day', 'day_hour') \
      .save( s3path )


# recreate partition
# create_partition_v2('db_dev_cdp_project', 'lcpr_offers_in', str(year)+str(month)+str(day))  

query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.lcpr_offers_in""")
print('Actualiza: db_dev_cdp_project.lcpr_offers_in')

job.commit()