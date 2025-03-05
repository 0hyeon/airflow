import requests
import json
import boto3
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from io import StringIO  # CSV 처리를 위해 필요

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# 날짜 설정
from_date = "2025-03-01"
to_date = "2025-03-01"

TOKEN = Variable.get("TOKEN")

HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}

# URL 설정
URLS = {
    "1pick_view_jobposting_AOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?from=2025-03-01&to=2025-03-01&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting&additional_fields=device_category"
}

# S3 설정
S3_BUCKET = "fc-practice2"
S3_KEY_PREFIX = "apps_flyer_data/"


# S3 저장 함수
def save_to_s3(records, filename):
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    s3_client = session.client("s3")

    file_path = (
        f"{S3_KEY_PREFIX}{filename}_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    )

    try:
        df = pd.read_csv(StringIO(records))  # CSV 변환
        temp_file = "/tmp/temp.parquet"
        df.to_parquet(temp_file, engine="pyarrow", index=False)

        # S3 업로드
        s3_client.upload_file(temp_file, S3_BUCKET, file_path)
        print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{file_path}")
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")


# API 데이터를 가져와 S3에 저장하는 함수
def fetch_and_save_data():
    for filename, url in URLS.items():
        try:
            print(f"📡 Fetching data from {url}")
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()

            # JSON이 아니라 CSV 형식일 가능성이 높음
            csv_data = response.text.strip()  # CSV 형식 데이터

            if not csv_data:
                print(f"⚠️ No data for {filename}, skipping.")
                continue

            save_to_s3(csv_data, filename)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data from {url}: {e}")


# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 2),
    "retries": 1,
}

with DAG(
    dag_id="daily_s3_storage",
    default_args=default_args,
    schedule_interval="@daily",  # 매일 실행
    catchup=False,
) as dag:

    task_fetch_and_save = PythonOperator(
        task_id="fetch_and_save_data",
        python_callable=fetch_and_save_data,
    )
