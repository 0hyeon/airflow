import requests
import json
import boto3
import pandas as pd
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# 날짜 계산 (월요일이면 3일치 가져오기, 아니면 1일치)
current_date = datetime.now()
current_day_of_week = current_date.weekday()
yesterday = current_date - timedelta(days=1)

from_date = "2025-03-01"
to_date = "2025-03-01"

TOKEN = Variable.get("TOKEN")

HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}


##
URLS = {
    "1pick_view_jobposting_AOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?from=2025-03-01&to=2025-03-01&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting&additional_fields=device_category",
    # "1pick_view_jobposting_iOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=1pick_view_jobposting",
    # "careercheck_assess_complete_AOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
    # "careercheck_assess_complete_iOS": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?from={from_date:%Y-%m-%d}&to={to_date:%Y-%m-%d}&timezone=Asia%2FSeoul&category=standard&event_name=careercheck_assess_complete",
}
#
# **S3 설정**
S3_BUCKET = "fc-practice2"
S3_KEY_PREFIX = "apps_flyer_data/"

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")
AWS_DEFAULT_REGION = Variable.get("AWS_DEFAULT_REGION")


# **S3 저장 함수**
def save_to_s3(records, filename):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_DEFAULT_REGION,
    )
    s3_client = session.client("s3")
    # s3_client = boto3.client("s3")
    file_path = (
        f"{S3_KEY_PREFIX}{filename}_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    )

    try:
        df = pd.DataFrame(records)
        temp_file = "/tmp/temp.parquet"
        df.to_parquet(temp_file, engine="pyarrow", index=False)

        s3_client.upload_file(temp_file, S3_BUCKET, file_path)
        print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{file_path}")
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")


# **API 데이터를 수집하여 S3에 저장**
def fetch_and_save_data():
    for filename, url in URLS.items():
        try:
            print(f"📡 Fetching data from {url}")
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

            if not data:
                print(f"⚠️ No data for {filename}, skipping.")
                continue

            print(f"✅ API 응답 데이터: {data[:5]}")  # 일부 데이터 출력
            print(f"✅ 데이터 타입: {type(data)}")

            save_to_s3(data, filename)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data from {url}: {e}")


# **Airflow DAG 설정**
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
