# docker build . -t 0hyeon/worker:0.1.1
# docker push 0hyeon/worker:0.1.1

FROM python:3.9

# 필수 패키지 설치 (한 번에 실행)
RUN apt-get update && apt-get install -y \
    vim \
    default-jdk && \
    rm -rf /var/lib/apt/lists/*  # ✅ 불필요한 패키지 캐시 제거

# 환경 변수 설정 (최신 표준 적용 ✅)
ENV JAVA_HOME=/usr/lib/jvm/default-java  

# Python 패키지 설치 (최적화된 버전)
RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    pandas==2.1.3 \
    konlpy==0.6.0 \
    boto3
