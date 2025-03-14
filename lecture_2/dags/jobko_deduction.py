from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
# 1. 날짜 설정 (어제 날짜)
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
S3_BUCKET = "fc-practice2"
S3_INPUT_PREFIX = f"apps_flyer_jobko/date={yesterday}/"
S3_OUTPUT_PREFIX = f"apps_flyer_jobko/deduction_results/"
AWS_REGION = "ap-northeast-2"

# 1_2. 클러스터생성 사양 
JOB_FLOW_OVERRIDES = {
    "Name": "fc_emr_cluster",
    "ReleaseLabel": "emr-6.10.0",  # ✅ EMR 버전 반영
    "Applications": [{"Name": "Spark"}],  # ✅ Spark 추가
    "Instances": {
        "InstanceFleets": [
            {
                "InstanceFleetType": "MASTER",
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "m5.xlarge",
                        "WeightedCapacity": 1
                    }
                ],
                "TargetOnDemandCapacity": 1
            },
            {
                "InstanceFleetType": "CORE",
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "m5.xlarge",
                        "WeightedCapacity": 1
                    }
                ],
                "TargetOnDemandCapacity": 1,
                "TargetSpotCapacity": 0
            },
            {
                "InstanceFleetType": "TASK",
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": "c5.xlarge",
                        "WeightedCapacity": 1
                    }
                ],
                "TargetSpotCapacity": 1
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": False,  # ✅ Auto-Terminate 설정
        "Ec2KeyName": "test",  # ✅ EC2 Key 설정
        "TerminationProtected": False,  # ✅ 종료 가능하도록 설정
    },
    "ManagedScalingPolicy": {  # ✅ Managed Scaling Policy 추가
        "ComputeLimits": {
            "UnitType": "InstanceFleetUnits",
            "MinimumCapacityUnits": 3,
            "MaximumCapacityUnits": 15,
            "MaximumCoreCapacityUnits": 10,
            "MaximumOnDemandCapacityUnits": 5
        }
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",  # ✅ use-default-roles 적용
    "ServiceRole": "EMR_DefaultRole",  # ✅ use-default-roles 적용
    "AutoTerminationPolicy": {"IdleTimeout": 3600},  # ✅ 1시간 후 자동 종료
    "LogUri": f"s3://{S3_BUCKET}/emr-logs/",  # ✅ 로그 저장 위치
    "Region": "ap-northeast-2"  # ✅ 추가
}


# 2. DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "jobko_deduction_to_s3_emr",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    on_name="ap-northeast-2",  # ✅ AWS 리전 추가
    dag=dag,
)

# 4. S3 파일 8개가 모두 존재하는지 확인하는 함수
def check_all_s3_files():
    
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name=Variable.get("AWS_DEFAULT_REGION"),
    )

    s3 = session.client("s3")
    
    files = [
        "data_aos_onepick_retarget.parquet",
        "data_aos_onepick_ua.parquet",
        "data_aos_retarget.parquet",
        "data_ios_onepick_retarget.parquet",
        "data_ios_onepick_ua.parquet",
        "data_ios_retarget.parquet",
        "data_ios_ua.parquet",
    ]

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_INPUT_PREFIX)

    if "Contents" not in response:
        raise AirflowException(f"❌ '{S3_INPUT_PREFIX}' 경로에 파일이 없음!")

    # ✅ S3에서 실제 존재하는 파일 목록 가져오기
    existing_files = {obj["Key"].split("/")[-1] for obj in response["Contents"]}
    
    # ✅ 필요한 파일과 비교하여 누락된 파일 확인
    missing_files = [file for file in files if file not in existing_files]

    if missing_files:
        raise AirflowException(f"❌ S3에 누락된 파일: {missing_files}")

    print(f"✅ S3에 모든 파일이 존재합니다.")


check_s3_files = PythonOperator(
    task_id="check_s3_files",
    python_callable=check_all_s3_files,
    dag=dag,
)

# 5. EMR에서 PySpark 작업 실행
run_emr_spark = EmrAddStepsOperator(
    task_id="run_emr_spark",
    job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",  # ✅ 수정
    aws_conn_id="aws_default",
    region_name="ap-northeast-2",  # ✅ AWS 리전 추가
    steps=[
        {
            "Name": "Process Parquet with PySpark",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "s3://fc-practice2/scripts/jobko_deduction.py",
                    f"s3://{S3_BUCKET}/{S3_INPUT_PREFIX}",
                    f"s3://{S3_BUCKET}/{S3_OUTPUT_PREFIX}"
                ],
            },
        }
    ],
    dag=dag,
)

# 6. 이전 DAG 실행
trigger_previous_dag = TriggerDagRunOperator(
    task_id="trigger_previous_dag",
    trigger_dag_id="jobko_apps_deduction_daily_to_s3_storage_sequential",
    dag=dag,
)

# 7. 이전 DAG 완료 확인
wait_for_previous_dag = ExternalTaskSensor(
    task_id="wait_for_previous_dag",
    external_dag_id="jobko_apps_deduction_daily_to_s3_storage_sequential",
    timeout=3600,
    mode="poke",
    poke_interval=60,
    dag=dag,
)

# 8. DAG 실행 순서 설정
trigger_previous_dag >> wait_for_previous_dag >> check_s3_files >> create_emr_cluster >> run_emr_spark
