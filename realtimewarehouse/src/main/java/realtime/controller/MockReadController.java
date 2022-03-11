package realtime.controller;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.CommonService;
import realtime.service.MockService;

public class MockReadController {
    public static CommonService commonService = new CommonService();
    public static MockService mockService = new MockService();
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);
        // 读数据
        mockService.readMock(env, tEnv);

        // 执行
        env.execute();
    }
}