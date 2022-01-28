package realtime.ddl;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建 topic=ods_ncddzt 的kafka表
 */
@Slf4j
public class KafkaOdsNcddztTable {
    public static void main(String[] args) {
        // flink 环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 建库
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tEnv.useDatabase("kafka");
        tEnv.executeSql("DROP TABLE IF EXISTS ods_ncddzt");
        // 建表
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
                "    'properties.group.id' = 'realtime',  -- reading from the beginning\n" +
                "    'scan.startup.mode' = 'latest-offset',  -- latest-offset\n" +
                "    'properties.bootstrap.servers' = 'hadoop01:9092',  -- kafka broker address\n" +
                "    'format' = 'json'  -- the data format is json\n" +
                ")";
        log.info("kafka table ods_ncddzt:\n"+kafkaOdsNcddzt);
        tEnv.executeSql(kafkaOdsNcddzt);
    }
}
