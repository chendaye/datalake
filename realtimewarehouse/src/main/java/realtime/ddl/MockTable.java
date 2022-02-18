package realtime.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.CommonService;
import realtime.service.MockService;

/**
 * 创建 ods_ncddzt 表
 */
public class MockTable {
    public static CommonService commonService = new CommonService();
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);

        MockService mockService = new MockService();
        mockService.createMockTable(tEnv);
    }
}

