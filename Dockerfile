FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

WORKDIR /app

RUN pip3 install --no-cache-dir boto3 psycopg2-binary sqlalchemy pandas pyarrow pyspark==3.5.3

# jdbc и hadoop-aws
RUN curl -sL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar -o /opt/spark/jars/postgresql-42.7.1.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

COPY config.py main.py python_etl.py /app/

ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
