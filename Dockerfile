FROM astrocrpublic.azurecr.io/runtime:3.1-2

# Install Spark + JRE without using apt/apk/yum (works on slim/minimal bases)
USER root

# 1) Download Temurin JRE 11 (ARM64) and Spark using Docker ADD
ADD https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.24+8/OpenJDK11U-jre_aarch64_linux_hotspot_11.0.24_8.tar.gz /tmp/jre.tgz
ADD https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz /tmp/spark.tgz

RUN set -eux; \
    mkdir -p /opt/java /opt && \
    tar -xzf /tmp/jre.tgz -C /opt/java --strip-components=1 && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm -f /tmp/jre.tgz /tmp/spark.tgz

RUN ln -s /opt/java/bin/java /usr/local/bin/java \
 && ln -s /opt/java/bin/keytool /usr/local/bin/keytool

# 2) Python libs for Spark runtime
RUN pip install --no-cache-dir pyspark==3.5.1 pyarrow==16.1.0 findspark==2.0.1

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin

USER astro