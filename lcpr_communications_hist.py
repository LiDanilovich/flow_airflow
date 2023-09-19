
from datetime import date, timedelta, datetime
import sys
import json
import boto3
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2
from awsglue.context import GlueContext

print("STARTING ...")

# el parámetro days_before se utiliza para setear la fecha a utilizar en la recuperación de datos
if ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
else:
    args = {'days_before': '1'}
    
days_before = int(args['days_before'])
# today se utiliza para filtrar comunicaciones (históricos de los canales xej callcenter_history) 
# y los feedbacks  (históricos de los canales xej feedback_email_sends o feedback_ivr)

today = (datetime.today()-timedelta(days=days_before)).strftime('%Y%m%d')
print('data for :', today)

year=today[0:4]
month=str(today)[4:6]
day=str(today)[6:8]

S3 = boto3.resource('s3')

s3pathparquet = 's3://cdp-lcpr-process/hist/communications_hist/'
print('path:',s3pathparquet)  



athena_view_dataframe \
    .write.mode('overwrite') \
    .format('parquet') \
    .save( s3pathparquet )
	
#query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")
print('Actualiza: db_dev_cdp_project.communications_hist')


 

job.commit()