package realtime.controller;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.CommonService;
import realtime.service.DwdService;

public class OdsToDwdController {
    public static CommonService commonService = new CommonService();
    public static DwdService dwdService = new DwdService();

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);
        // 创建DWD表
        dwdService.createDwdTable(tEnv);
        // ods -> dwd
        dwdService.insertToDwd(env, tEnv);
    }
}
