# Flink 1.18 + MySQL CDC / JDBC + 同步 UDF（VT 令牌化）
FROM maven:3.9-eclipse-temurin-17 AS udf-build
WORKDIR /build
COPY udf/pom.xml udf/pom.xml
COPY udf/src udf/src
RUN mvn -f udf/pom.xml -q package -DskipTests

FROM flink:1.18-scala_2.12-java17

USER root

ARG FLINK_CDC_VERSION=3.1.1
ARG FLINK_JDBC_VERSION=3.1.2-1.18
ARG MYSQL_DRIVER_VERSION=8.3.0

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fL -o /opt/flink/lib/flink-sql-connector-mysql-cdc-${FLINK_CDC_VERSION}.jar \
         https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/${FLINK_CDC_VERSION}/flink-sql-connector-mysql-cdc-${FLINK_CDC_VERSION}.jar \
    && curl -fL -o /opt/flink/lib/flink-connector-jdbc-${FLINK_JDBC_VERSION}.jar \
         https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${FLINK_JDBC_VERSION}/flink-connector-jdbc-${FLINK_JDBC_VERSION}.jar \
    && curl -fL -o /opt/flink/lib/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar \
         https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar \
    && chown -R flink:flink /opt/flink/lib

COPY --from=udf-build /build/udf/target/flink-sync-udf.jar /opt/flink/lib/flink-sync-udf.jar
RUN chown flink:flink /opt/flink/lib/flink-sync-udf.jar

COPY scripts/flink-docker-entrypoint.sh /docker-entrypoint.d/00-fix-checkpoints.sh
RUN chmod +x /docker-entrypoint.d/00-fix-checkpoints.sh

USER root
ENTRYPOINT ["/docker-entrypoint.d/00-fix-checkpoints.sh"]
