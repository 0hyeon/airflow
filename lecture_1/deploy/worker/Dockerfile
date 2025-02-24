# docker build . -t byeongjokim/worker:0.1.0
# docker push byeongjokim/worker:0.1.0

FROM python:3.9

RUN apt-get update && apt-get install -y vim && apt-get install -y default-jdk 
ENV JAVA_HOME /usr/lib/jvm/default-java

RUN pip install confluent-kafka==2.3.0 pandas==2.1.3 konlpy==0.6.0 tensorflow==2.13.0