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
'''
args = getResolvedOptions(sys.argv, ['bucket','prefix','key'])
bucket = args['bucket']
prefix = args['prefix']
key = args['key']
'''
bucket = 'cdp-lcpr-feedbacks-bucket'
prefix = 'outbound/'
key = 'outbound_feedback_offerfit_campaign_20230901.csv'

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
          concat('LCPR_FX_',cuenta) account_id,
         cast(cuenta as string) cuenta,
         to_date(Intento_discado_1,'MM/dd/yyyy') Intento_discado_1,
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


def communication_history():
    
    # get communications for a day = today
    comm = spark.sql(f""" SELECT * from db_dev_cdp_project.communications_hist where day='{str(year)+str(month)+str(day)}' and channel='Outbound_call' """)
    comm.createOrReplaceTempView("comm")
    print("comm df has ", str(comm.count()), " rows ") 

    # contactos x Outbound_call
    outbound = spark.sql(f""" select account_id account, min(Intento_discado_1) dt_contact
                              from  db_dev_cdp_project.feedback_cc_out 
                              where day='{str(year)+str(month)+str(day)}' and intento_discado_1 is not null
                                    and (Disposicion  like '%OFITi- Cliente acepta oferta%' or 
                                        Disposicion like '%Cliente no acepta ninguna oferta%')
                              group by account_id          
                        """)
                        
    outbound.createOrReplaceTempView("outbound")
    print("outbound df has ", str(outbound.count()), " rows ") 
                        
    newcomm = comm.join(outbound, (comm["account_id"] == outbound["account"])  & (comm["comm_dt"] ==  outbound["dt_contact"])   ,"left")
    print(newcomm.count()) 
    
    nc = newcomm.withColumn("contact_dt",F.when((F.col("account_id")==F.col("account")) & F.col("contact_dt").isNull(),F.col("dt_contact")).otherwise(F.col("contact_dt")))
    
    nc=nc.drop("account","dt_contact")
    nc.createOrReplaceTempView("nc")
    
    if nc.count() > 0:
       s3path_hst = "s3://cdp-lcpr-process/hist/communications_hist/"   
       nc = nc.repartition("month", "day", "channel")
       nc \
           .write.mode('overwrite') \
           .format('parquet') \
           .partitionBy('month', 'day', 'channel') \
           .save( s3path_hst )	
           
       query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")

def ingest_comm_hist():
    today_year='2023'
    today_month='09'
    today_day='01'
    
    news = spark.sql(f""" select distinct 
                          concat('LCPR_FX_', cast(lcpr_lla_id as string)) account_id
                          ,cast(lcpr_lla_id  as string) sub_acct_no_sbb
                          ,cast(null as string) use_case
                          ,campaign_name
                          ,cast(null as bigint) contact_id
                          ,cast(null as bigint) campaign_id
                          ,to_date('{str(today_year)+str(today_month)+str(today_day)}','yyyyMMd') comm_dt
                          ,cast(null as timestamp) send_dt
                          ,cast(null as timestamp) cancel_dt
                          ,cast(null as string) cancel_reason
                          ,cast(null as timestamp) bounce_dt 
                          ,cast(null as string) dsn_reason
                          ,cast(null as string) bounce_type
                          ,cast(null as timestamp) open_dt 
                          ,cast(null as timestamp) click_dt 
                          ,cast(null as integer) clicks_count
                          ,cast(null as timestamp) contact_dt 
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
                          ,'{str(today_year)+str(today_month)}' as month
                          ,'{str(today_year)+str(today_month)+str(today_day)}' as day
                          ,'Outbound_call' channel
                          from db_dev_cdp_project.hist_emarsys_cc_in
                          where day='{str(today_year)+str(today_month)+str(today_day)}' """)
                          
                          
    news.createOrReplaceTempView("news")
    print("news df has ", str(news.count()), " rows ") 
    if news.count() > 0:
       s3path_hst = "s3://cdp-lcpr-process/hist/communications_hist/"    
       news = news.repartition("month", "day", "channel")

       news \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'channel') \
        .save( s3path_hst )


       query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")



#.................... actualiza historico de feedback
feedback_history()

#.................... actualiza en histórico de comunicaciones la contactación
communication_history()

#ingest_comm_hist()

job.commit()