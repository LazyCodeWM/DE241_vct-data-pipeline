FROM apache/airflow:2.9.1-python3.11

USER root

# Install Java 17 (required by PySpark 3.5.x)
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically (path differs between amd64 and arm64)
RUN JAVA_PATH=$(readlink -f /usr/bin/java | sed 's|/bin/java||') && \
    echo "JAVA_HOME=${JAVA_PATH}" >> /etc/environment && \
    echo "export JAVA_HOME=${JAVA_PATH}" > /etc/profile.d/java.sh && \
    echo "export PATH=\${JAVA_HOME}/bin:\${PATH}" >> /etc/profile.d/java.sh

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install pipeline dependencies (airflow already in base image)
RUN pip install --no-cache-dir \
    python-dotenv==1.0.0 \
    vlrdevapi \
    pydantic==2.7.0 \
    boto3==1.34.0 \
    pyspark==3.5.1 \
    polars==0.20.0 \
    "pyiceberg[s3filesystem,nessie]==0.7.0" \
    pyarrow==15.0.0
