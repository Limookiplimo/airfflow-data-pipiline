import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


#Connect postgresql
def queryPostgresql():
    conn_string = "dbname='testdb' host='localhost' user='user' password='password'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select name, city from users", conn)
    df.to_csv('postgresqldata.csv')
    print("=======Done========")


#connect Elastisearch
def insertElasticsearch():
    es = Elasticsearch("http://localhost:9200")
    df = pd.read_csv('postgresqldata.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql", doc_type="doc", body=doc)
        print(res)



#dag arguments
dag_args = {
    "owner":"limoo",
    "start_date":dt.datetime(2023,2,11),
    "retries":1,
    "retry_delay":dt.timedelta(minutes=5),
}

#define dags
with DAG("DbDag",
            default_args=dag_args,
            schedule_interval=timedelta(minutes=5),
            #'0 * * * *',
            )as dag:
            getData = PythonOperator(task_id = "QueryPostgreSQL", python_callable=queryPostgresql)
            insertData = PythonOperator(task_id = "InsertDataElasticsearch", python_callable=insertElasticsearch)

getData >> insertData