import requests
import boto3
import pandas as pd
import os
from datetime import datetime, timedelta
from io import StringIO  # CSV 처리를 위해 필요

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
##
# 날짜 설정
current_date = datetime.now()
yesterday = current_date - timedelta(days=1)

TOKEN = Variable.get("TOKEN")
HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}

# URL 목록 (순서대로 실행)
URLS = {
    "data_aos_onepick_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard&event_name=1pick_view_jobposting",
    "data_aos_onepick_retarget": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard&event_name=1pick_view_jobposting",
    "data_aos_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard",
    "data_aos_retarget": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in-app-events-retarget/v5?timezone=Asia%2FSeoul&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard",
    "data_ios_onepick_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&event_name=1pick_view_jobposting",
    "data_ios_onepick_retarget": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?timezone=Asia%2FSeoul&category=standard&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&event_name=1pick_view_jobposting",
    "data_ios_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard",
    "data_ios_retarget": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in-app-events-retarget/v5?timezone=Asia%2FSeoul&category=standard&from={yesterday:%Y-%m-%d}&to={yesterday:%Y-%m-%d}&category=standard",
}

# S3 설정
S3_BUCKET = "fc-practice2"
S3_KEY_PREFIX = "apps_flyer_data/"

# S3 저장 함수 (Parquet 저장만)
def save_to_s3(records, filename):
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )
    s3_client = session.client("s3")

    # S3에 날짜별 폴더 생성하여 저장
    file_path_parquet = f"{S3_KEY_PREFIX}{yesterday:%Y-%m-%d}/{filename}.parquet"

    try:
        # CSV 데이터 변환
        df = pd.read_csv(StringIO(records), encoding="utf-8-sig")

        if df.empty:
            print(f"⚠️ DataFrame is empty for {filename}, skipping S3 upload.")
            return

        # 임시 Parquet 파일 생성
        temp_file_parquet = f"/tmp/{filename}.parquet"
        df.to_parquet(temp_file_parquet, engine="pyarrow", index=False)

        # S3 업로드
        s3_client.upload_file(temp_file_parquet, S3_BUCKET, file_path_parquet)
        print(f"✅ Parquet 업로드 완료: s3://{S3_BUCKET}/{file_path_parquet}")

        # 임시 파일 삭제
        os.remove(temp_file_parquet)

    except Exception as e:
        print(f"❌ Error saving to S3: {e}")

# API 데이터를 가져와 S3에 저장하는 함수 (개별 실행)
def fetch_and_save_data(filename, url):
    try:
        print(f"📡 Fetching data from {url}")
        response = requests.get(url, headers=HEADERS, timeout=60)  # 타임아웃 추가
        response.raise_for_status()

        # JSON이 아니라 CSV 형식으로 응답될 가능성이 높음
        csv_data = response.text.strip()

        if not csv_data:
            print(f"⚠️ No data for {filename}, skipping.")
            return

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
    dag_id="daily_s3_storage_sequential",
    default_args=default_args,
    schedule_interval="@daily",  # 매일 실행
    catchup=False,
) as dag:

    # 첫 번째 Task
    previous_task = None

    for index, (filename, url) in enumerate(URLS.items()):
        task = PythonOperator(
            task_id=f"fetch_and_save_{index}",
            python_callable=fetch_and_save_data,
            op_args=[filename, url],
        )

        if previous_task:
            previous_task >> task  # 순차적으로 실행
        previous_task = task

