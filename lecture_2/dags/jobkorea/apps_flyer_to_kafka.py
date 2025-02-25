import os
import json
import requests
import pandas as pd
import boto3
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Kafka 설정
KAFKA_BROKER = "broker:29092"
KAFKA_TOPIC = "apps_flyer_data"

# S3 설정
S3_BUCKET = "fc-practice2"
S3_KEY_PREFIX = "apps_flyer_data/"

# AWS
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# 헤더
TOKEN = os.getenv("JOBKOREA_TOKEN")
HEADERS = {"accept": "text/csv", "authorization": f"Bearer {TOKEN}"}

# 날짜관련
current_date = datetime.now()
current_day_of_week = current_date.weekday()
yesterday = current_date - timedelta(days=1)

if current_day_of_week == 0:  # 월요일이면 3일치 가져오기
    from_date = current_date - timedelta(days=3)
else:
    from_date = yesterday

to_date = yesterday

# AppsFlyer API URL 리스트 (AOS + iOS)
URLS = [
    f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting",
    # f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting",
    # f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
    # f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
]


# AppsFlyer API에서 데이터 가져오기
def fetch_data():
    all_data = []
    for url in URLS:
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.text.split("\n")  # CSV 데이터 줄 단위로 변환
            all_data.extend(data)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data: {e}")
    return all_data


# Kafka Producer: 데이터를 Kafka로 전송
def stream_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    data = fetch_data()

    if not data:
        print("⚠️ No data fetched from AppsFlyer")
        return

    for record in data:
        producer.send(KAFKA_TOPIC, record)

    producer.flush()  # 주기적으로 flush() 실행
    producer.close()
    print(f"✅ Kafka로 {len(data)}건의 데이터 전송 완료")


# Kafka Consumer: 데이터를 받아 S3에 Parquet 저장
def consume_and_save_to_s3():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    batch_size = 1000
    records = []

    for message in consumer:
        records.append(message.value)
        if len(records) >= batch_size:
            df = pd.DataFrame(records)
            save_to_s3(df)
            records = []

    if records:  # 남아있는 데이터 저장
        df = pd.DataFrame(records)
        save_to_s3(df)


# Parquet 파일을 S3에 저장 (메모리에서 직접 S3로 업로드)
def save_to_s3(df):
    s3_client = boto3.client("s3")

    file_path = f"{S3_KEY_PREFIX}{datetime.now().strftime('%Y-%m-%d')}.parquet"

    with open("/tmp/temp.parquet", "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False)

    s3_client.upload_file("/tmp/temp.parquet", S3_BUCKET, file_path)

    print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{file_path}")


# Airflow DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "catchup": False,
}

with DAG(
    "apps_flyer_to_kafka",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    task_fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch_data)

    task_stream_kafka = PythonOperator(
        task_id="stream_to_kafka", python_callable=stream_to_kafka
    )

    task_consume_s3 = PythonOperator(
        task_id="consume_and_save_to_s3", python_callable=consume_and_save_to_s3
    )

    task_fetch_data >> task_stream_kafka >> task_consume_s3
