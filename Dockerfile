FROM apache/airflow:2.10.5

# Права администратора
USER root

# Обновление и установка пакетных менеджеров
RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y wget &&\
    apt-get install -y openssh-server

RUN mkdir /var/run/sshd

# Set a simple password for the root user (for demonstration only)
# In production, use key-based authentication
RUN echo 'root:your_secure_password' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\\s*required\\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
EXPOSE 22
# Start the SSH server when the container launches
CMD ["/usr/sbin/sshd", "-D"]


# Установка JDK
RUN wget https://download.oracle.com/java/22/archive/jdk-22.0.2_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar -xvf jdk-22.0.2_linux-x64_bin.tar.gz -C /opt/java && \
    rm jdk-22.0.2_linux-x64_bin.tar.gz

# Установка JAVA
RUN wget -O jre-8u431-linux-x64.tar.gz https://javadl.oracle.com/webapps/download/AutoDL?BundleId=251398_0d8f12bc927a4e2c9f8568ca567db4ee && \
    tar -xvf jre-8u431-linux-x64.tar.gz -C /opt/java && \
    rm jre-8u431-linux-x64.tar.gz

# # Установка Apache Spark
# RUN wget https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
#     mkdir -p /opt/spark && \
#     tar -xvf spark-3.5.4-bin-hadoop3.tgz -C /opt/spark && \
#     rm spark-3.5.4-bin-hadoop3.tgz
RUN mkdir -p /opt/spark
RUN mkdir -p /opt/spark/spark-3.5.4-bin-hadoop3
RUN mkdir -p /opt/spark/spark-3.5.4-bin-hadoop3/jars

# загрузка JDBC-драйверов для PostgreSQL и ClickHouse и перенос их в папку jars
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && \
    wget https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.4/clickhouse-jdbc-0.6.4-all.jar && \
    mv postgresql-42.7.4.jar /opt/spark/spark-3.5.4-bin-hadoop3/jars && \
    mv clickhouse-jdbc-0.6.4-all.jar /opt/spark/spark-3.5.4-bin-hadoop3/jars

# Возвращение к пользователю по умолчанию
USER airflow

# Установка переменных окружения
ENV JAVA_HOME=/opt/java/jdk-22.0.2
ENV SPARK_HOME=/opt/spark/spark-3.5.4-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.8-src.zip

# Установка остальных пакетов через pip
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt