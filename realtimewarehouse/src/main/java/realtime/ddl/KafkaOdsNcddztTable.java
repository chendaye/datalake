package realtime.ddl;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.OdsService;

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
        KafkaOdsNcddztTable kafkaOdsNcddztTable = new KafkaOdsNcddztTable();
        OdsService odsService = new OdsService();
        odsService.createKafkaTable(tEnv);
    }

}