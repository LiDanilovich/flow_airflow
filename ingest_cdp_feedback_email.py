
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta, datetime
import json
import boto3
import pandas as pd
import requests

from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2,  get_auth_emarsys
from CDPPy.staging_queries import *
from pyspark.sql import functions as F

print("STARTING ...")

# el parámetro days_before se utiliza para setear el feedback de qué día voy a ingestar.   Por default es 1 (feedback de ayer)
if ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
else:
    args = {'days_before': '1'}
    
days_before = int(args['days_before'])

today = (datetime.today()-timedelta(days=days_before)).strftime('%Y-%m-%d')
hoy = (datetime.today()-timedelta(days=days_before)).strftime('%Y%m%d')
print('buscamos feedback del ',today)

year=today[0:4]
month=str(today)[5:7]
day=str(today)[8:10]

#settings for api's Emarsys
api_username = "lilac006"
api_secret = "eY33nysDDM8oVu2Ackh7"

#list of program_id for cdp's automation programs

url = 'https://api.emarsys.net/api/v2/ac/programs'
headers =  get_auth_emarsys(api_username, api_secret)
payload={}
response = requests.get(url, headers= headers , data=json.dumps(payload))
resp_dict = response.json()

programsID='('
for k in resp_dict['data']:
    if ( '_CDP_' in k['name']  ) :
       programsID = programsID + k['id'] + ',' 
programsID = programsID + ')'    
programsID = programsID.replace(',)',')')
print(programsID)

# settings for big query
session = boto3.session.Session()
client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1')
        
client = session.client(service_name='secretsmanager', region_name='us-east-1')
creds = client.get_secret_value(SecretId="bigq_sap_od_sapodlilac")
service_account_info = json.loads(json.loads(creds['SecretString'], strict=False)['credentials'])

credentials = service_account.Credentials.from_service_account_info(service_account_info)

clientBQ = bigquery.Client(credentials=credentials)

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def ingest_feedback_email(mySql,mySchema,myPath,mySource):
    if mySource == 'bigquery':
       query_job = clientBQ.query(mySql)
       pandasDF = query_job.to_dataframe()
       sparkDF=spark.createDataFrame(pandasDF,schema=mySchema)
    else:
       sparkDF=spark.sql(mySql)
    
    sparkDF.createOrReplaceTempView("sparkDF")
    finalDF = spark.sql(f""" select *, 
             '{str(year)+str(month)}' as month,
             '{str(year)+str(month)+str(day)}' as day 
             from sparkDF """)

    finalDF.show(3, truncate=False)
    print("Cant.Registros=",(finalDF.count()))
    
    if finalDF.count() > 0:
       finalDF = finalDF.repartition("month", "day")

       finalDF \
         .write.mode('overwrite') \
         .format('parquet') \
         .partitionBy('month', 'day') \
         .save( myPath )
      
    return    
  


# def for email_campaigns

sql_campaigns = """ 
select id, name, version_name, language, category_id, parent_campaign_id, type, sub_type, program_id, partitiontime, event_time, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` 
WHERE date(event_time) = '{}' and program_id in  {}
""".format(today,programsID)

schema_campaigns = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("version_name", StringType(), True),
    StructField("language", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("parent_campaign_id", LongType(), True) ,
    StructField("type", StringType(), True),
    StructField("sub_type", StringType(), True),
    StructField("program_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_campaigns = "s3://cdp-lcpr-process/em/feedback/campaigns"

# .................................................................................................................
# def for email sends

sql_sends = """
select contact_id, launch_id, campaign_type,  domain, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_sends_837621947` 
WHERE contact_id is not null and  date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)


schema_sends = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_sends = "s3://cdp-lcpr-process/em/feedback/sends"


# .................................................................................................................

# def for email opens
sql_opens = """
select contact_id, launch_id, domain, email_sent_at, campaign_type, geo, platform, md5, is_mobile, is_anonymized, uid, ip, user_agent, generated_from, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_opens_837621947` 
WHERE contact_id is not null and  date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)

schema_opens = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField(
        "geo",
        StructType([
            StructField("postal_code", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("accuracy_radius", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("country_iso_code", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("time_zone", StringType(), True),
        ]),
        True
        ),
    StructField("platform", StringType(), True),
    StructField("md5", StringType(), True),
    StructField("is_mobile", StringType(), True),
    StructField("is_anonymized", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("generated_from", StringType(), True),
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_opens = "s3://cdp-lcpr-process/em/feedback/opens"

# .................................................................................................................
# def for email bounces

sql_bounces = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, bounce_type,
campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at, dsn_reason
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_bounces_837621947` 
WHERE contact_id is not null and date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)



schema_bounces = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("bounce_type", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True),
    StructField("dsn_reason", StringType(), True),
    ])


s3path_bounces = "s3://cdp-lcpr-process/em/feedback/bounces"

# .................................................................................................................
# def for email cancels

sql_cancels = """
select contact_id, launch_id, reason, campaign_type, suite_type, suite_event,
campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_cancels_837621947` 
WHERE contact_id is not null and date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)

schema_cancels = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("suite_type", StringType(), True),
    StructField("suite_event", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_cancels = "s3://cdp-lcpr-process/em/feedback/cancels"

# .................................................................................................................
# def for email clicks

sql_clicks = """
select contact_id, launch_id, domain, email_sent_at, campaign_type, geo, platform, md5, is_mobile, is_anonymized, uid, ip, user_agent, section_id, link_id, category_id, is_img, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at, category_name,link_name, link_analysis_name,relative_link_id
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_clicks_837621947` 
WHERE contact_id is not null and date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)


schema_clicks = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField(
        "geo",
        StructType([
            StructField("postal_code", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("accuracy_radius", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("country_iso_code", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("time_zone", StringType(), True),
        ]),
        True
        ),
    StructField("platform", StringType(), True),
    StructField("md5", StringType(), True),
    StructField("is_mobile", StringType(), True),
    StructField("is_anonymized", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("section_id", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("category_id", StringType(), True) ,
    StructField("is_img", StringType(), True) ,
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True),
    StructField("category_name", StringType(), True) ,
    StructField("link_name", StringType(), True) ,
    StructField("link_analysis_name", StringType(), True) ,
    StructField("relative_link_id", StringType(), True)
    ])


s3path_clicks = "s3://cdp-lcpr-process/em/feedback/clicks"

# .................................................................................................................
# def for email complaints
sql_complaints = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_complaints_837621947` 
WHERE contact_id is not null and date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)

schema_complaints = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])

s3path_complaints = "s3://cdp-lcpr-process/em/feedback/complaints"

# .................................................................................................................
# def for email unsubscribes
sql_unsubscribes = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, source, campaign_id, message_id, event_time, customer_id, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_unsubscribes_837621947` 
WHERE contact_id is not null and date(event_time) = '{}' and campaign_id in 
 ( select id from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` where program_id in {} )
""".format(today,programsID)



schema_unsubscribes = StructType([
    StructField("contact_id", StringType(), True),
    StructField("launch_id", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])

s3path_unsubscribes = "s3://cdp-lcpr-process/em/feedback/unsubscribes"
# .................................................................................................................
# realiza ingesta
'''
ingest_feedback_email(mySql=sql_campaigns,mySchema=schema_campaigns,myPath=s3path_campaigns,mySource='bigquery')
ingest_feedback_email(mySql=sql_sends,mySchema=schema_sends,myPath=s3path_sends,mySource='bigquery')
ingest_feedback_email(mySql=sql_opens,mySchema=schema_opens,myPath=s3path_opens,mySource='bigquery')
ingest_feedback_email(mySql=sql_bounces,mySchema=schema_bounces,myPath=s3path_bounces,mySource='bigquery')
ingest_feedback_email(mySql=sql_cancels,mySchema=schema_cancels,myPath=s3path_cancels,mySource='bigquery')
ingest_feedback_email(mySql=sql_clicks,mySchema=schema_clicks,myPath=s3path_clicks,mySource='bigquery')
ingest_feedback_email(mySql=sql_complaints,mySchema=schema_complaints,myPath=s3path_complaints,mySource='bigquery')
ingest_feedback_email(mySql=sql_unsubscribes,mySchema=schema_unsubscribes,myPath=s3path_unsubscribes,mySource='bigquery')

# actualiza particiones
create_partition_v2('db_dev_cdp_project', 'feedback_email_campaigns', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_sends', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_opens', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_bounces', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_cancels', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_clicks', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_complaints', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_unsubscribes', str(year)+str(month)+str(day))
'''
# actualiza histórico de communicaciones email 
# .................................................................................................................
def communications_of_the_day():
    # get communications for a day = today
    comm = spark.sql(f""" SELECT * from db_dev_cdp_project.communications_hist where day='{hoy}' and channel='email' """)
    comm.createOrReplaceTempView("comm")
    print("comm df has ", str(comm.count()), " rows ") 
    
    email_sends = spark.sql(f"""
                  select b.account_id     
                  , b.sub_acct_no_sbb
                  , b.channel
                  , o.use_case
                  , b.campaign_name
                  , b.contact_id
                  , b.campaign_id
                  , b.comm_dt
                  , b.sent_dt 
                  , b.cancel_dt
                  , b.cancel_reason
                  , b.bounce_dt
                  , b.dsn_reason
                  , b.bounce_type 
                  , b.open_dt
                  , b.click_dt 
                  , b.clicks_count 
                  , b.contact_dt
                  , o.id_oferta_int
                  , o.offer_type
                  , l.link 
                  , l.link_generated_dt
                  , l.link_expiration_dt 
                  , cast(null as string) order_id 
                  , cast(null as string) conv_use_case_cat
                  , cast(null as timestamp) conversion_dt
                  , cast(null as string) conversion_type
                  from 
                      (select i.externalid  account_id, 
                       substring(i.externalid, 9,20) sub_acct_no_sbb,
                       'email'  channel, 
                        cast(null as string) campaign_name,
                        s.contact_id, 
                        s.campaign_id, 
                        cast(s.event_time  as date) comm_dt, 
                        s.event_time sent_dt, 
                        cast(null as timestamp) cancel_dt,
                        cast(null as string) cancel_reason,
                        cast(null as timestamp) bounce_dt,
                        cast(null as string) dsn_reason,
                        cast(null as string) bounce_type, 
                        cast(null as timestamp) open_dt,
                        cast(null as timestamp) click_dt, 
                        cast(null as integer) clicks_count, 
                        cast(null as timestamp) contact_dt
                        from db_dev_cdp_project.feedback_email_sends s 
                        left join db_dev_cdp_project.emarsys_id i  on s.contact_id=i.user_id
                        where  s.contact_id is not null and cast(s.event_time as date) =  to_date('{hoy}', 'yyyyMMd')
                        UNION 
                        select i.externalid  account_id, 
                        substring(i.externalid, 9,20) sub_acct_no_sbb,
                        'email'  channel, 
                        cast(null as string) campaign_name,
                        s.contact_id, 
                        s.campaign_id, 
                        cast(s.event_time  as date) comm_dt, 
                        cast(null as timestamp) sent_dt, 
                        s.event_time cancel_dt,
                        s.reason cancel_reason,
                        cast(null as timestamp) bounce_dt,
                        cast(null as string) dsn_reason,
                        cast(null as string) bounce_type, 
                        cast(null as timestamp) open_dt,
                        cast(null as timestamp) click_dt, 
                        cast(null as integer) clicks_count,  
                        cast(null as timestamp) contact_dt
                        from db_dev_cdp_project.feedback_email_cancels s 
                        left join db_dev_cdp_project.emarsys_id i  on s.contact_id=i.user_id
                        where s.contact_id is not null and cast(s.event_time as date) =  to_date('{hoy}', 'yyyyMMd') 
                  ) b                   
                  left join ( select t1.sub_acct_no, t1.offer_type, t1.use_case, t1.day, t1.id_oferta_int
                              from db_dev_cdp_project.lcpr_offers_in t1
                              inner join (select concat('LCPR_FX_',sub_acct_no) sub_acct_no, offer_type, channel, use_case, max(day) max_day
                                          from db_dev_cdp_project.lcpr_offers_in
                                          where day  <= '{hoy}'
                                          group BY sub_acct_no, offer_type, channel, use_case
                                         ) t2 on t1.sub_acct_no=t2.sub_acct_no and t1.offer_type=t2.offer_type 
                                             and t1.use_case=t2.use_case and t1.day=t2.max_day and t1.channel=t2.channel and
                                             t1.channel = 'email' and t1.offer_type = 'single'
                            ) o on o.sub_acct_no = b.account_id 
       
                  left join db_dev_cdp_project.hist_links_output l on o.id_oferta_int= l.id_oferta_int
                  """)
    
                           
    email_sends.show(3, truncate=False)
    email_sends.createOrReplaceTempView("email_sends")
    print("sends + cancels df has ", str(email_sends.count()), " rows ") 

    finalDF = spark.sql(f"""
                   select b.*,
                   '{str(year)+str(month)}' as month,
                   '{str(year)+str(month)+str(day)}' as day
                   from email_sends b 
                   """)

    finalDF.createOrReplaceTempView("finalDF")
    
    if finalDF.count() > 0:
       finalDF = finalDF.repartition("month", "day", "channel")
       finalDF \
           .write.mode('overwrite') \
           .format('parquet') \
           .partitionBy('month', 'day', 'channel') \
           .save( s3path_hst )	
           
       query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")

# ....................................................................................................................

def update_from_opens():
     # get communications for a day = today
    comm = spark.sql(f""" SELECT * from db_dev_cdp_project.communications_hist where day='{hoy}' and channel='email' """)
    comm.createOrReplaceTempView("comm")
    print("comm df has ", str(comm.count()), " rows ") 

    email_opens = spark.sql(f""" SELECT o.contact_id contact_id_open
                                 , o.email_sent_at
                                 , cast(o.campaign_id as long) campaign_id_open
                                 , min(event_time) event_time
                                 FROM db_dev_cdp_project.feedback_email_opens o 
                                 INNER JOIN  db_dev_cdp_project.communications_hist c
                                 ON (c.contact_id = o.contact_id) and c.campaign_id  = cast(o.campaign_id as long) and cast(o.email_sent_at as date) =  to_date('{hoy}', 'yyyyMMd') 
                                 WHERE datediff(cast(o.event_time as date),cast(o.email_sent_at as date)) <= 14
                                 group by contact_id_open, email_sent_at, campaign_id_open
                            """)
                               
    email_opens.show(3, truncate=False)
    email_opens.createOrReplaceTempView("email_opens")
    print("email_opens df has ", str(email_opens.count()), " rows ")   

    # set emails opens
    newcomm = comm.join(email_opens, (comm["contact_id"] == email_opens["contact_id_open"])  & (comm["campaign_id"] == email_opens["campaign_id_open"])  & (comm["channel"] == 'email')  ,"left")
    
    nc = newcomm.withColumn("open_dt",F.when( (F.col("contact_id") == F.col("contact_id_open")) & F.col("open_dt").isNull() ,F.col("event_time")).otherwise(F.col("open_dt")))   
    
    nc=nc.drop("contact_id_open","email_sent_at","campaign_id_open","event_time")
    nc.createOrReplaceTempView("nc")
    
    nc = nc.repartition("month", "day", "channel")
    nc \
           .write.mode('overwrite') \
           .format('parquet') \
           .partitionBy('month', 'day', 'channel') \
           .save( s3path_hst )	
           
    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")

# ....................................................................................................................
def update_from_clicks():
    
    # get communications for a day = today
    comm = spark.sql(f""" SELECT * from db_dev_cdp_project.communications_hist where day='{hoy}' and channel='email' """)
    comm.createOrReplaceTempView("comm")
    print("comm df has ", str(comm.count()), " rows ") 

    
    email_clicks = spark.sql(f""" SELECT o.contact_id contact_id_click
                                 , o.email_sent_at
                                 , cast(o.campaign_id as long) campaign_id_click
                                 , min(event_time) event_time
                                 , count(*) count_clicks
                                 FROM db_dev_cdp_project.feedback_email_clicks o 
                                 INNER JOIN  db_dev_cdp_project.communications_hist c
                                 ON (c.contact_id = o.contact_id) and c.campaign_id  = cast(o.campaign_id as long) and cast(o.email_sent_at as date) =  to_date('{hoy}', 'yyyyMMd') 
                                 WHERE datediff(cast(o.event_time as date),cast(o.email_sent_at as date)) <= 14
                                 group by contact_id_click, email_sent_at, campaign_id_click
                            """)
                               
    email_clicks.show(3, truncate=False)
    email_clicks.createOrReplaceTempView("email_clicks")
    print("email_clicks df has ", str(email_clicks.count()), " rows ")   

    
    
    # set emails opens
    newcomm = comm.join(email_clicks, (comm["contact_id"] == email_clicks["contact_id_click"])  & (comm["campaign_id"] == email_clicks["campaign_id_click"])  & (comm["channel"] == 'email')  ,"left")
    
    nc = newcomm.withColumn("click_dt",F.when( (F.col("contact_id") == F.col("contact_id_click")) & F.col("click_dt").isNull() ,F.col("event_time")).otherwise(F.col("click_dt"))) \
       .withColumn("clicks_count",F.when( (F.col("contact_id") == F.col("contact_id_click")) ,F.col("count_clicks")).otherwise(F.col("clicks_count"))) 
    
    nc=nc.drop("contact_id_click","email_sent_at","campaign_id_click","event_time")
    nc.createOrReplaceTempView("nc")
   
    nc = nc.repartition("month", "day", "channel")

    nc \
           .write.mode('overwrite') \
           .format('parquet') \
           .partitionBy('month', 'day', 'channel') \
           .save( s3path_hst )	
           
    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")

 
# ....................................................................................................................
def update_from_bounces():
    
    # get communications for a day = today
    comm = spark.sql(f""" SELECT * from db_dev_cdp_project.communications_hist where day='{hoy}' and channel='email' """)
    comm.createOrReplaceTempView("comm")
    print("comm df has ", str(comm.count()), " rows ") 

    
    email_bounces = spark.sql(f""" select b1.contact_id contact_id_bounce, b1.campaign_id campaign_id_bounce, b1.email_sent_at, b1.event_time, b1.bounce_type type, b1.dsn_reason reason
                                   from db_dev_cdp_project.feedback_email_bounces b1
                                   inner join (select contact_id, campaign_id, email_sent_at, min(event_time) event_time_bounce
                                                  from db_dev_cdp_project.feedback_email_bounces 
                                                  group by contact_id, campaign_id, email_sent_at) b2
                                      on b1.contact_id=b2.contact_id and b1.campaign_id=b2.campaign_id and b1.event_time=b2.event_time_bounce
                                   inner join   db_dev_cdp_project.communications_hist c
                                      on (c.contact_id = b1.contact_id) and c.campaign_id  = cast(b1.campaign_id as long) and cast(b1.email_sent_at as date) =  to_date('{hoy}', 'yyyyMMd') 
                                   where datediff(cast(b1.event_time as date),cast(b1.email_sent_at as date)) <= 14
                              """)
                               
    email_bounces.show(3, truncate=False)
    email_bounces.createOrReplaceTempView("email_bounces")
    print("email_bounces df has ", str(email_bounces.count()), " rows ")   

    # set emails bounces
    newcomm = comm.join(email_bounces, (comm["contact_id"] == email_bounces["contact_id_bounce"])  & (comm["campaign_id"] == email_bounces["campaign_id_bounce"])  & (comm["channel"] == 'email')  ,"left")
    
    nc = newcomm.withColumn("bounce_dt",F.when( (F.col("contact_id") == F.col("contact_id_bounce")) & F.col("bounce_dt").isNull() ,F.col("event_time")).otherwise(F.col("bounce_dt"))) \
        .withColumn("bounce_type",F.when( (F.col("contact_id") == F.col("contact_id_bounce")) ,F.col("type")).otherwise(F.col("bounce_type"))) \
        .withColumn("dsn_reason",F.when( (F.col("contact_id") == F.col("contact_id_bounce")) ,F.col("reason")).otherwise(F.col("dsn_reason"))) 
  
    nc=nc.drop("contact_id_bounce","email_sent_at","campaign_id_bounce","event_time","type","reason")
    nc.createOrReplaceTempView("nc")
   
    nc = nc.repartition("month", "day", "channel")

    nc \
           .write.mode('overwrite') \
           .format('parquet') \
           .partitionBy('month', 'day', 'channel') \
           .save( s3path_hst )	 
    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")

           
# ....................................................................................................................

s3path_hst = "s3://cdp-lcpr-process/hist/communications_hist/"

# ...............   update from the events
communications_of_the_day()


#update_from_opens()

#update_from_clicks()

#update_from_bounces()

# ............... refresh
query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.communications_hist""")
print('Actualiza: db_dev_cdp_project.communications_hist')

job.commit()

