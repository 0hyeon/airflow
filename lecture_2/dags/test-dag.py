import requests
import boto3
import pandas as pd
import os
from datetime import datetime, timedelta
from io import StringIO

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# 날짜 설정
current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')  # ✅ 문자열로 변환

TOKEN = Variable.get("TOKEN")
HEADERS = {"accept": "application/json", "authorization": f"Bearer {TOKEN}"}

# URL 목록 (순서대로 실행)
URLS = {
    "data_aos_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/com.jobkorea.app/in_app_events_report/v5?timezone=Asia%2FSeoul&from={yesterday_str}&to={yesterday_str}&category=standard&event_name=careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,1st_update_complete,signup_all,job_apply_complete_homepage,job_apply_complete_mobile,re_update_complete,resume_complete_1st,resume_complete_re,minikit_complete,jobposting_complete,1pick_direct_apply,business_signup_complete,1pick_offer_apply,1pick_start&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
    "data_ios_ua": f"https://hq1.appsflyer.com/api/raw-data/export/app/id569092652/in_app_events_report/v5?timezone=Asia%2FSeoul&category=standard&from={yesterday_str}&to={yesterday_str}&event_name=careercheck_assess_complete,high_view_jobposting,high_scrap_notice,high_direct_apply,high_homepage_apply,event_2411_tvc_apply,nhis_resistration_complete,1st_update_complete,job_apply_complete_homepage,job_apply_complete_mobile,resume_complete_re,signup_all,re_update_complete,minikit_complete,resume_complete_1st,jobposting_complete,1pick_direct_apply,business_signup_complete,1pick_offer_apply,1pick_start&additional_fields=device_category,match_type,conversion_type,campaign_type,device_model,keyword_id",
}

# S3 설정
S3_BUCKET = "fc-practice2"
S3_KEY_PREFIX = "apps_flyer_data/"

# Airflow DAG 설정
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "execution_timeout": timedelta(hours=2),  # ✅ 실행 시간 연장
}

with DAG(
    dag_id="jobko_apps_deduction_daily_to_s3_storage_sequential",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    previous_task = None

    for index, (filename, url) in enumerate(URLS.items()):
        task = KubernetesPodOperator(
            task_id=f"fetch_and_save_{index}",
            name=f"fetch-and-save-data-{index}",
            namespace="default",  # ✅ namespace 변경 (필요하면 "airflow"로 수정)
            image="0hyeon/hyeon-airflow-image:latest",  # ✅ 사용자 정의 이미지 사용
            cmds=["python", "-c"],
            arguments=[
                f"""
import requests
import boto3
import pandas as pd
import os
from io import StringIO
from datetime import datetime, timedelta

# 날짜 설정
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

TOKEN = os.getenv("TOKEN")
HEADERS = {{"accept": "application/json", "authorization": f"Bearer {TOKEN}"}}
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX")

url = os.getenv("URL")
filename = os.getenv("FILENAME")

try:
    print(f"📡 Fetching data from {url}")
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    csv_data = response.text.strip()

    if not csv_data:
        print(f"⚠️ No data for {filename}, skipping.")
        exit(0)

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    s3_client = session.client("s3")

    df = pd.read_csv(StringIO(csv_data), encoding="utf-8-sig", low_memory=False, dtype={{'Postal Code': str}})

    if df.empty:
        print(f"⚠️ DataFrame is empty for {filename}, skipping S3 upload.")
        exit(0)

    temp_file_parquet = f"/tmp/{filename}.parquet"
    df.to_parquet(temp_file_parquet, engine="pyarrow", index=False)

    file_path_parquet = f"{{S3_KEY_PREFIX}}date={{yesterday_str}}/{{filename}}.parquet"
    s3_client.upload_file(temp_file_parquet, S3_BUCKET, file_path_parquet)

    print(f"✅ Parquet 업로드 완료: s3://{{S3_BUCKET}}/{{file_path_parquet}}")
    os.remove(temp_file_parquet)
    exit(0)

except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching data from {url}: " + str(e))
    exit(1)
                """
            ],
            is_delete_operator_pod=True,  # ✅ 실행 후 Pod 자동 삭제
            get_logs=True,
            in_cluster=True,
            config_file=None,
            env_vars={
                "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY"),
                "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_KEY"),
                "AWS_DEFAULT_REGION": Variable.get("AWS_DEFAULT_REGION"),
                "TOKEN": TOKEN,
                "S3_BUCKET": S3_BUCKET,
                "S3_KEY_PREFIX": S3_KEY_PREFIX,
                "URL": url,
                "FILENAME": filename,
            },
        )

        if previous_task:
            previous_task >> task  # ✅ 순차 실행
        previous_task = task
