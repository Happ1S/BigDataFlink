FROM flink:1.17.1-scala_2.12-java11

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev gcc libpq-dev wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink/jars
RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN wget -q -O /opt/flink/jars/flink-sql-connector-kafka-1.17.1.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar && \
    wget -q -O /opt/flink/jars/flink-connector-jdbc-3.1.0-1.17.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.17/flink-connector-jdbc-3.1.0-1.17.jar && \
    wget -q -O /opt/flink/jars/postgresql-42.5.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

USER flink