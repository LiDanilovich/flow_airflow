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
from CDPPy.job_functions import *
from CDPPy.staging_queries import *

print("STARTING ...")

# el parámetro days_before se utiliza para setear el feedback de qué día voy a ingestar.   Por default es 1 (feedback de ayer)
if ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
else:
    args = {'days_before': '0'}
    
days_before = int(args['days_before'])

today = (datetime.today()-timedelta(days=days_before)).strftime('%Y-%m-%d')
print('buscamos feedback del ',today)

year=today[0:4]
month=str(today)[5:7]
day=str(today)[8:10]

#settings for api's Emarsys
api_username = "lilac006"
api_secret = "eY33nysDDM8oVu2Ackh7"

def get_fields_emarsys_v2():
  url = 'https://api.emarsys.net/api/v2/field/translate/en'
  payload = {}
  response = requests.get(url, headers= get_auth_emarsys(api_username, api_secret) , data=json.dumps(payload))
  jsoned2 = str(response.text)
  print(jsoned2)
#   df = spark.read.json(sc.parallelize([jsoned2]))
#   df2 = df.withColumn("id", monotonically_increasing_id()).withColumn("string_id", monotonically_increasing_id()) \
#              .select("id", "string_id", explode("data").alias("col")) \
#              .select("col.*")
#   field_ids = df2.select("id", "string_id").rdd.collect()
#   field_ids_sorted = sorted(field_ids, key=lambda x: x.id)
#   field_ids_sorted.sort()
#   return field_ids_sorted


def test_return_contact_list():
    #fields = get_fields_emarsys_v2()
    field_id = 864
    multi = 0
    offset_id = 0
    schema = StructType([
        StructField("id", StringType(), True),
        StructField(f"{field_id}", StringType(), True)
    ])
    dfs = spark.createDataFrame([], schema=schema)
    # for field in fields:
        # field_id = field.id
    while True:
        url = f'https://api.emarsys.net/api/v2/contact/query/'
        payload = {
            "709":"",
            "return":field_id,
            "offset":offset_id
        }
        try:
            response = requests.get(url, headers= get_auth_emarsys(api_username, api_secret) , params=payload)
            respuesta = json.loads(response.text.encode('utf8'))['data']['result']
            df = spark.createDataFrame(respuesta, schema=schema)
            df.show(10, truncate=False)
            print(str(df.count()))
            if df.count() > 0:
                dfs = dfs.union(df)
                dfs.show(10, truncate=False)
                print(str(dfs.count()))
                multi = multi + 1
                offset_id = 10000 * multi
            else:
                break
        except Exception as e:
            print({str(e)})
    
    print(str(dfs.count()))
    dfs_dedup = dfs.filter(col("864").like('LCPR%')).distinct()
    print(str(dfs_dedup.count()))
    
def test_segment_api():
    segmentId = 13641
    url = f'https://api.emarsys.net/api/v2/filter/{segmentId}/runs'
    payload = {}
    response = requests.post(url, headers=get_auth_emarsys(api_username, api_secret), data=json.dumps(payload))
    respuesta = json.loads(response.text.encode('utf8'))['data']
    print(str(respuesta))

def test_segment_status_api():
    runId = '20230822203133-i3wM7XnK-suite42-web01a-65382-s42'
    url = f'https://api.emarsys.net/api/v2/filter/runs/{runId}'
    print(url)
    payload = {}
    response = requests.get(url, headers=get_auth_emarsys(api_username, api_secret), data=json.dumps(payload))
    respuesta = json.loads(response.text.encode('utf8'))['data']
    print(str(respuesta))

def test_get_contact_data():
    url = f'https://api.emarsys.net/api/v2/contact/getdata'
    payload = {
        "keyId": "868",
        "keyValues": ["LCPR"],
        "fields": ["1", "2", "3"]
    }
    try:
        response = requests.post(url, headers= get_auth_emarsys(api_username, api_secret) , params=payload)
        respuesta1=str(response.text)
        print(respuesta1)
        respuesta = json.loads(response.text.encode('utf8'))['data']['result']
    except Exception as e:
        print({str(e)})

# Obtener el DataFrame resultante
# result_df = test_return_contact_list()
# result_df.show()

#test_segment_api()

#test_segment_status_api()

#get_fields_emarsys_v2()
#create_segment_emarsys()

test_return_contact_list()
#test_get_contact_data()

job.commit()