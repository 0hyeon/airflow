import argparse
import requests
import json
import boto3
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer

from airflow import DAG
from airflow.operators.python import PythonOperator

# 날짜 계산 (월요일이면 3일치 가져오기, 아니면 1일치)
current_date = datetime.now()
current_day_of_week = current_date.weekday()
yesterday = current_date - timedelta(days=1)
# from_date = yesterday if current_day_of_week != 0 else current_date - timedelta(days=3)
# to_date = yesterday
from_date = "2025-03-01"
to_date = "2025-03-01"

TOKEN = os.getenv("JOBKOREA_TOKEN")
HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}

# **URL 리스트 (각 URL 별로 저장)**
URLS = {
    "1pick_view_jobposting_AOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting",
    # "1pick_view_jobposting_iOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting",
    # "careercheck_assess_complete_AOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
    # "careercheck_assess_complete_iOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
}


# **Kafka Producer 설정**
def create_kafka_producer(brokers):
    return Producer({"bootstrap.servers": brokers})


# **Kafka Consumer 설정**
def create_kafka_consumer(brokers, topic, group_id="fetch-group"):
    conf = {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer


# **URL별 데이터 수집 후 Kafka에 전송**
def stream_to_kafka():
    producer = create_kafka_producer("kafka-service:9092")

    for filename, url in URLS.items():
        try:
            print(f"📡 Fetching data from {url}")
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()  # JSON 데이터

            if not data:
                print(f"⚠️ No data for {filename}, skipping.")
                continue

            # **Kafka로 전송 (URL 별로 데이터 전송)**
            for record in data:
                producer.produce(
                    "category-match-out",
                    json.dumps({"filename": filename, "data": record}),
                )
                producer.poll(0)

            producer.flush()
            print(f"✅ Kafka로 {len(data)}건 전송 완료 ({filename})")

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data from {url}: {e}")


# **Kafka Consumer가 URL 단위로 S3에 저장**
def consume_and_save_to_s3():
    consumer = create_kafka_consumer("kafka-service:9092", "category-match-out")
    records_by_filename = {}

    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            time.sleep(1)
            continue

        message = json.loads(msg.value())
        filename = message["filename"]
        record = message["data"]

        # **파일명별로 데이터를 그룹화하여 저장**
        if filename not in records_by_filename:
            records_by_filename[filename] = []

        records_by_filename[filename].append(record)

        # **URL 단위로 S3에 저장 (Kafka에서 받은 모든 데이터 저장)**
        if len(records_by_filename[filename]) > 0:
            save_to_s3(
                records_by_filename[filename],
                "fc-practice2",
                "apps_flyer_data/",
                filename,
            )
            records_by_filename[filename] = []  # 저장 후 초기화


# **S3 저장 함수**
def save_to_s3(records, s3_bucket, s3_key_prefix, filename):
    s3_client = boto3.client("s3")
    file_path = (
        f"{s3_key_prefix}{filename}_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    )

    try:
        df = pd.DataFrame(records)
        with open("/tmp/temp.parquet", "wb") as f:
            df.to_parquet(f, engine="pyarrow", index=False)

        s3_client.upload_file("/tmp/temp.parquet", s3_bucket, file_path)
        print(f"✅ S3 업로드 완료: s3://{s3_bucket}/{file_path}")
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")


# **Airflow DAG 설정**
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 2),
    "retries": 1,
}
#
with DAG(
    dag_id="daily_kafka_to_s3",
    default_args=default_args,
    schedule_interval="@daily",  # 매일 실행
    catchup=False,
) as dag:

    # **Kafka로 데이터 전송**
    task_stream_kafka = PythonOperator(
        task_id="stream_to_kafka",
        python_callable=stream_to_kafka,
    )

    # **Kafka 메시지를 소비하여 S3에 저장**
    task_consume_s3 = PythonOperator(
        task_id="consume_and_save_to_s3",
        python_callable=consume_and_save_to_s3,
    )

    task_stream_kafka >> task_consume_s3  # 실행 순서 지정
# import datetime

# from airflow import DAG
# from airflow.operators.bash import BashOperator

# with DAG(
#     dag_id="test_dag",
#     start_date=datetime.datetime(2023, 11, 19),
#     schedule_interval="@daily",
# ) as dag:

#     step1 = BashOperator(task_id="print_date", bash_command="date")

#     step2 = BashOperator(task_id="echo", bash_command="echo 123")

#     step1 >> step2
