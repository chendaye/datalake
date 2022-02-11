package realtime.controller;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.CommonService;
import realtime.service.DwsService;

public class DwdToDwsController {
    public static CommonService commonService = new CommonService();
    public static DwsService dwsService = new DwsService();

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);
        // 创建dws表
        dwsService.createDwsTable(tEnv);
        // dwd -> dws
        dwsService.insertToDws(env, tEnv);
    }
}
