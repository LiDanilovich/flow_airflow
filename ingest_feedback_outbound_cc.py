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

# el parámetro days_before se utiliza para setear el  día voy a ingestar.   Por default es 1 (feedback de ayer)
if ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
else:
    args = {'days_before': '0'}
    
days_before = int(args['days_before'])

today = (datetime.today()-timedelta(days=days_before)).strftime('%Y%m%d %H%M%S')

year=today[0:4]
month=str(today)[4:6]
day=str(today)[6:8]
hour_min_sec = str(today)[9:15]

args = getResolvedOptions(sys.argv, ['bucket','prefix','key'])
bucket = args['bucket']
prefix = args['prefix']
key = args['key']

campaign_name = key.replace('outbound_feedback_','').replace('.csv','')


print("the source is: ",bucket,prefix,key)

# Create a GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


# .............  inserta en feedback_cc_out

def feedback_history():

    theFile = 's3://' + bucket + '/' + prefix + key
    df = spark.read.option("header","true").option("delimiter", ",").csv(theFile)
    df.createOrReplaceTempView("df")


    formatteddf = spark.sql(f""" 
         select 
         concat('LCPR_FX_',Cuenta) account_id,
         cast(Cuenta as string) cuenta,
         to_date(Intento_discado_1,'MM/dd/yyyy')  Intento_discado_1,
         to_date(Intento_discado_2,'MM/dd/yyyy')  Intento_discado_2,
         to_date(Intento_discado_3,'MM/dd/yyyy')  Intento_discado_3,
         Hora_llamada,
         Disposicion,
         Comentarios,
         '{key}' csv_origen,
         '{str(year)+str(month)}' month, 
         '{str(year)+str(month)+str(day)}' day,
         '{str(year)+str(month)+str(day)+str(hour_min_sec)}' as day_hour
         from df  """)
   
    formatteddf.printSchema()
    formatteddf.show(3, truncate=False)

    formatteddf.createOrReplaceTempView("formatteddf")

    s3path = "s3://cdp-lcpr-process/hist/feedback_cc_out/"
    
    if formatteddf.count() > 0:
       formatteddf = formatteddf.repartition("month", "day", "day_hour")

       formatteddf \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'day_hour') \
        .save( s3path )


       query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.feedback_cc_out""")
       print('Actualiza: db_dev_cdp_project.lcpr_offers_in')




def ingest_comm_hist():
   
    news = spark.sql(f""" select distinct account_id
                          ,cast(Cuenta  as string) sub_acct_no_sbb
                          ,cast(null as string) use_case
                          ,'{campaign_name}' campaign_name
                          ,cast(null as bigint) contact_id
                          ,cast(null as bigint) campaign_id
                          ,comm_dt
                          ,cast(comm_dt as timestamp) sent_dt
                          ,cast(null as timestamp) cancel_dt
                          ,cast(null as string) cancel_reason
                          ,cast(null as timestamp) bounce_dt 
                          ,cast(null as string) dsn_reason
                          ,cast(null as string) bounce_type
                          ,cast(null as timestamp) open_dt 
                          ,cast(null as timestamp) click_dt 
                          ,cast(null as integer) clicks_count
                          ,cast(contact_dt as timestamp) contact_dt
                          ,cast(null as integer) id_oferta_int
                          ,cast(null as string) offer_type
                          ,cast(null as date) rec_date
                          ,cast(null as string) link 
                          ,cast(null as date) link_generated_dt
                          ,cast(null as date) link_expiration_dt
                          ,cast(null as string) order_id
                          ,cast(null as string) conv_use_case_cat
                          ,cast(null as timestamp) conversion_dt
                          ,cast(null as string) conversion_type
                          ,cast(null as integer) program_id
                          ,False as conv_flg
                          ,replace(substring(string(comm_dt),1,7),'-','') month
                          ,replace(string(comm_dt),'-','') day
                          ,'Outbound_call' channel
                          from 
                          ( Select account_id,
                                   Cuenta, 
                                   Intento_discado_1 comm_dt,
                                   (case when ((Disposicion  like '%OFITi- Cliente acepta oferta%' or 
                                        Disposicion like '%Cliente no acepta ninguna oferta%')) and Intento_discado_1 >= coalesce(Intento_discado_2,Intento_discado_3,Intento_discado_1)  then Intento_discado_1 else cast(null as date) end) contact_dt
                            from db_dev_cdp_project.feedback_cc_out
                            where Intento_discado_1 is not null  and day='{str(year)+str(month)+str(day)}'
                            UNION 
                            Select account_id,
                                   Cuenta, 
                                   Intento_discado_2 comm_dt,
                                   (case when ((Disposicion  like '%OFITi- Cliente acepta oferta%' or 
                                        Disposicion like '%Cliente no acepta ninguna oferta%')) and Intento_discado_2 >= coalesce(Intento_discado_3,Intento_discado_1,Intento_discado_2)  then Intento_discado_2 else cast(null as date) end) contact_dt
                            from db_dev_cdp_project.feedback_cc_out
                            where Intento_discado_2 is not null  and day='{str(year)+str(month)+str(day)}' and intento_discado_2 <> Intento_discado_1
                            UNION 
                            Select account_id,
                                   Cuenta, 
                                   Intento_discado_3 comm_dt,
                                   (case when ((Disposicion  like '%OFITi- Cliente acepta oferta%' or 
                                        Disposicion like '%Cliente no acepta ninguna oferta%')) and Intento_discado_3 >= coalesce(Intento_discado_2,Intento_discado_1,Intento_discado_3)  then Intento_discado_3 else cast(null as date) end) contact_dt
                            from db_dev_cdp_project.feedback_cc_out
                            where Intento_discado_3 is not null and day='{str(year)+str(month)+str(day)}'  and Intento_discado_3 <> Intento_discado_2
                          )  """)
                          
                        
                          
                          
    news.createOrReplaceTempView("news")
    print("news df has ", str(news.count()), " rows ") 
    if news.count() > 0:
       s3path_hst = "s3://cdp-lcpr-process/hist/communications_hist/"    
       news = news.repartition("month", "day", "channel")

       news \
        .write.mode('append') \
        .format('parquet') \
        .partitionBy('month', 'day', 'channel') \
        .save( s3path_hst )


       query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")



#.................... actualiza historico de feedback
feedback_history()

#.................... actualiza el histórico de comunicaciones 
ingest_comm_hist()



job.commit()