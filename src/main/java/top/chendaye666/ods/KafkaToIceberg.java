package top.chendaye666.ods;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

@Slf4j
public class KafkaToIceberg {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkoutpoint
    env.enableCheckpointing(5000);
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));

    EnvironmentSettings blinkStreamSettings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,blinkStreamSettings);
    System.setProperty("HADOOP_USER_NAME", "root");

    // 创建 hadoop catalog
    String hadoopCatalogSql = "CREATE CATALOG hadoop_catalog WITH (\n" +
        "  'type'='iceberg',\n" +
        "  'catalog-type'='hadoop',\n" +
        "  'warehouse'='hdfs://hadoop01:8020/warehouse/path',\n" +
        "  'property-version'='1'\n" +
        ")";
    log.error("iceberg hadoop_catalog:\n"+hadoopCatalogSql);
    tEnv.executeSql(hadoopCatalogSql);
    // use catalog
    tEnv.useCatalog("hadoop_catalog");
    // 建数据库
    tEnv.executeSql("CREATE DATABASE IF NOT EXISTS hadoop_catalog.ods");
    tEnv.useDatabase("ods");
    // 建表
    tEnv.executeSql("DROP TABLE IF EXISTS ods_ncddzt");
    String odsNcddztSql = "CREATE TABLE  ods_ncddzt (\n" +
        "   source_type STRING,\n" +
        "   `index` STRING,\n" +
        "   `agent_timestamp` STRING,\n" +
        "   source_host STRING,\n" +
        "   topic STRING,\n" +
        "   num INT,\n" +
        "   file_path STRING,\n" +
        "   `position` STRING,\n" +
        "   log STRING\n" +
        ")";
    log.error("iceberg table ods_ncddzt:\n"+odsNcddztSql);
    tEnv.executeSql(odsNcddztSql);

    //使用Hive Catalog创建Kafka流表(注意不要和 hive 创建的 iceberg 表混了，可以放在不通的库)
    HiveCatalog hiveCatalog =new HiveCatalog("kafka_hive_catalog", null, "D:\\code\\datalake\\src\\main\\resources",
        "2.1.1");
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
    log.error("kafka table ods_ncddzt:\n"+kafkaOdsNcddzt);
    tEnv.executeSql(kafkaOdsNcddzt);



    //使用SQL连接kafka流表和iceberg 目标表
    String sinkSql = "INSERT INTO  hadoop_catalog.ods.ods_ncddzt" +
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
    log.error("sinkSql:\n"+sinkSql);

    tEnv.executeSql(sinkSql);
    env.execute();
  }
}
