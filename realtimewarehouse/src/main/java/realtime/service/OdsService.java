package realtime.service;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsService {

    public void createKafkaTable(StreamTableEnvironment tEnv){
        // 建库
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tEnv.useDatabase("kafka");
        tEnv.executeSql("DROP TABLE IF EXISTS ods_ncddzt");
        // 建表
        String kafkaOdsNcddzt = "CREATE TABLE IF NOT EXISTS ods_ncddzt (\n" +
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
                "    'properties.group.id' = 'realtime',  -- reading from the beginning\n" +
                "    'scan.startup.mode' = 'latest-offset',  -- latest-offset\n" +
                "    'properties.bootstrap.servers' = 'hadoop01:9092',  -- kafka broker address\n" +
                "    'format' = 'json'  -- the data format is json\n" +
                ")";
        tEnv.executeSql(kafkaOdsNcddzt);
    }

    public void createOdsTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.ods_ncddzt (\n" +
                "    `source_type` STRING,\n" +
                "    `index` STRING,\n" +
                "    `agent_timestamp` STRING,\n" +
                "    `source_host` STRING,\n" +
                "    `topic` STRING,\n" +
                "    `num` BIGINT ,\n" +
                "    `file_path` STRING,\n" +
                "    `position` STRING,\n" +
                "    `log` STRING\n" +
                ") PARTITIONED BY (`topic`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }

    public void insertToOds(StreamTableEnvironment tEnv){
        String sinkSql = "INSERT INTO  hadoop_prod.realtime.ods_ncddzt" +
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
                "kafka.ods_ncddzt";


        tEnv.executeSql(sinkSql);
    }
}
