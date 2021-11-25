package top.chendaye666.v2;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * ods：Kafka 数据入 Iceberg
 *  /opt/flink-1.12.5/bin/flink run -t yarn-per-job --detached  /opt/work/datalake/ods-1.0-SNAPSHOT.jar
 */
@Slf4j
public class KafkaToSparkIceberg {
  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkoutpoint
    env.enableCheckpointing(5000);
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    //todo: 创建 hadoop catalog
    String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
        "  'type'='iceberg',\n" +
        "  'catalog-type'='hadoop',\n" +
        "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
        "  'property-version'='1'\n" +
        ")";
    log.info("iceberg hadoop_prod:\n"+hadoopCatalogSql);
    tEnv.executeSql(hadoopCatalogSql);
   

    //todo: 建Kafka表 使用Hive Catalog创建Kaf-ka流表(注意不要和 hive 创建的 iceberg 表混了，可以放在不同的库)
//    HiveCatalog hiveCatalog =new HiveCatalog("kafka_hive_catalog", null, "ods/src/main/resources",
//        "3.1.2");
    HiveCatalog hiveCatalog =new HiveCatalog("kafka_hive_catalog", null, "/opt/hive-3.1.2/conf",
        "3.1.2");
    tEnv.registerCatalog("kafka_hive_catalog", hiveCatalog);
    tEnv.executeSql("use catalog kafka_hive_catalog");
    tEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
    tEnv.useDatabase("kafka");
    tEnv.executeSql("DROP TABLE IF EXISTS ods_ncddzt");
    String kafkaOdsNcddzt = "CREATE TABLE ods_ncddzt (\n" +
        "    SOURCE_TYPE STRING,\n" +
        "    `INDEX` STRING,\n" +
        "    SOURCE_HOST STRING,\n" +
        "    TOPIC STRING,\n" +
        "    FILE_PATH STRING,\n" +
        "    `POSITION` STRING,\n" +
        "    `AGENT_TIMESTAMP` STRING,\n" +
        "    `NUM` INT,\n" +
        "    LOG STRING,\n" +
        "    --AGENT_TIMESTAMP TIMESTAMP(3),\n" +
        "    proctime AS PROCTIME()   -- generates processing-time attribute using computed column\n" +
        "    --WATERMARK FOR AGENT_TIMESTAMP AS AGENT_TIMESTAMP - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute\n" +
        ") WITH (\n" +
        "    'connector' = 'kafka',  -- using kafka connector\n" +
        "    'topic' = 'ods_ncddzt',  -- kafka topic\n" +
        "    'properties.group.id' = 'my_group',  -- reading from the beginning\n" +
        "    'scan.startup.mode' = 'latest-offset',  -- latest-offset\n" +
        "    'properties.bootstrap.servers' = 'hadoop01:9092',  -- kafka broker address\n" +
        "    'format' = 'json'  -- the data format is json\n" +
        ")";
    log.info("kafka table ods_ncddzt:\n"+kafkaOdsNcddzt);
    tEnv.executeSql(kafkaOdsNcddzt);

    //todo: 使用SQL连接kafka流表和iceberg 目标表
    String sinkSql = "INSERT INTO  hadoop_prod.ods.ncddzt" +
        " SELECT " +
        "SOURCE_TYPE as source_type ," +
        " `INDEX` as index, " +
        "AGENT_TIMESTAMP as agent_timestamp, " +
        "SOURCE_HOST as source_host," +
        "TOPIC as topic," +
        "NUM as `num`," +
        "FILE_PATH as file_path," +
        "`POSITION` as `position`," +
        "LOG as log" +
        " FROM " +
        "kafka_hive_catalog.kafka" +
        ".ods_ncddzt";

    log.info("sinkSql:\n" + sinkSql+"\n 这里运行的是 ODS ODS ODS ODS ODS ODS ODS ODS ODS ODS ODS ODS ODS ODS \n\n ");

    tEnv.executeSql(sinkSql);
//     env.execute("ods-iceberg");
  }
}
