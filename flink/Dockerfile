FROM flink:1.17.1-scala_2.12

# 1. Conector Kafka
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# 2. Conector JDBC Nativo de Flink
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar

# 3. Driver MySQL (Nuevo nombre oficial en Maven: mysql-connector-j)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar

WORKDIR /opt/flink