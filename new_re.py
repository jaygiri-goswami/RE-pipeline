import requests
import time
from operator import index
from google.cloud import bigquery
import os
import pandas as pd
from datetime import date, timedelta


# function to call both API & download CSV file
def api_to_csv_download():
    print("Calling first API : ")
    json_of_api1={
                "sourceType": "druid",
                "dataSource": "userEvents",
                "datasetType": "default",
                "filters": [
                    {
                        "type": "interval",
                        "comparator": "previous",
                        "operands": {
                        "_1": "__time",
                        "_2": {
                        "count": 1,
                        "type": "day", 
                        "includeCurrent": "true",
                        }
                    }
}
                ]
            }
    header_of_api1={
                    "x-api-key": "9L0h5yzZmNhwcBz7y40vULRadVuxGwIMfagWTki2",
                    "Content-Type":"application/json"
                   }

    response = requests.post('https://cloud.yellow.ai/api/insights/data-explorer/export?bot=x1619080924183&timeZone=Asia/Calcutta',headers=header_of_api1,json=json_of_api1)
    x=response.json()
    jobId=x['data']['jobId']
    #fetch jobId from API response
    print("JobId is : {}".format(jobId))
    print("Calling 2nd API and wait for the status to become success")

    #calling 2nd API
    for i in range(100):
        url2 ="https://cloud.yellow.ai/api/export/job/detail/{}?bot=x1619080924183".format(jobId)
        header_of_api2={
                        "x-api-key": "9L0h5yzZmNhwcBz7y40vULRadVuxGwIMfagWTki2"
                       }
        response = requests.post(url2,headers = header_of_api2)
        x=response.json()
        status=x['data']['status']
        if status=="SUCCESS": break 
        else: 
            print(status)
            time.sleep(40)
    x=response.json()
    url=x['data']['url']
    print("URL : ",url)
    # download csv from URL
    r = requests.get(url, allow_redirects=True)
    open('re.csv', 'wb').write(r.content)
    print("Downloaded the re.csv file")

    # Adding column of t-1 day
    df = pd.read_csv("re.csv")
    today = date.today()
    yesterday = today - timedelta(days = 1)
    df.insert(loc=0, column='date', value=yesterday.strftime("%Y-%m-%d"))
    df.to_csv('re1.csv', index=False)


#function to load CSV to BQ 
def csv_to_bq():
    print("Now loading data from csv to BigQuery ")    
    # from google.oauth2 import service_account
    gcp_project = 'robust-seat-338513'
    bd_dataset = 'dataframe_operation_perform'

    credentials_path = '/home/jaygirigoswami/Downloads/robust-seat-338513-e1cb147a2181.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema = [
                    bigquery.SchemaField("t_1_date", "DATE"),
                    bigquery.SchemaField("__time", "STRING"),
                    bigquery.SchemaField("bid", "STRING"),
                    bigquery.SchemaField("category", "STRING"),
                    bigquery.SchemaField("city", "STRING"),
                    bigquery.SchemaField("country", "STRING"),
                    bigquery.SchemaField("customId1", "STRING"),
                    bigquery.SchemaField("customId2", "STRING"),
                    bigquery.SchemaField("device", "STRING"),
                    bigquery.SchemaField("event", "STRING"),
                    bigquery.SchemaField("eventInfo", "STRING"),
                    bigquery.SchemaField("journey", "STRING"),
                    bigquery.SchemaField("platform", "STRING"),
                    bigquery.SchemaField("step", "STRING"),
                    bigquery.SchemaField("region", "STRING"),
                    bigquery.SchemaField("source", "STRING"),
                    bigquery.SchemaField("targetInfo", "STRING"),
                    bigquery.SchemaField("uid", "STRING"),
                    bigquery.SchemaField("sessionId", "STRING"),
                    bigquery.SchemaField("language", "STRING")
                ],write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    # time_partitioning = bigquery.TimePartitioning(field='t_1_date')
                    #     field="date",
                    #     # range_=bigquery.PartitionRange(start=0, end=100, interval=1)
                    #     )
    )
    job_config.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="t_1_date",  # name of column to use for partitioning
    # expiration_ms=7776000000,
    )  # 90 days
    # table = client.get_table("robust-seat-338513.dataframe_operation_perform.re")
    # job_config.time_partitioning = bigquery.TimePartitioning(field='t_1_date')
    with open("/home/jaygirigoswami/newproj/workspace/RE_PIPELINE/re1.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, "robust-seat-338513.dataframe_operation_perform.re" , job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table("robust-seat-338513.dataframe_operation_perform.re")
    print("Data loaded successfully to BigQuery")
    # os.remove("/home/jaygirigoswami/newproj/workspace/RE_PIPELINE/re.csv")
    # os.remove("/home/jaygirigoswami/newproj/workspace/RE_PIPELINE/re1.csv")

# api_to_csv_download()
csv_to_bq()

