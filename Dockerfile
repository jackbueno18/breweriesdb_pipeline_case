FROM astrocrpublic.azurecr.io/runtime:3.1-14

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# hadoop-aws + aws sdk jars for PySpark <-> MinIO (S3-compatible)
RUN mkdir -p /opt/spark/jars && \
    curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER astro


