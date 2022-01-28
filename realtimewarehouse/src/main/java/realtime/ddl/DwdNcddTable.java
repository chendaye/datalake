package realtime.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.DwdService;

/**
 * 创建 dwd_ncddzt 表
 */
public class DwdNcddTable {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DwdService dwdService = new DwdService();
        dwdService.createDwdTable(tEnv);
    }
}
