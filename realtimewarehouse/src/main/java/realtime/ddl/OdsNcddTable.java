package realtime.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.OdsService;

/**
 * 创建 ods_ncddzt 表
 */
public class OdsNcddTable {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        OdsService odsService = new OdsService();
        odsService.createOdsTable(tEnv);
    }
}
