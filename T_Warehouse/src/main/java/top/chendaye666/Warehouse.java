package top.chendaye666;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.Service.WarehouseTableService;
import top.chendaye666.utils.JsonParamUtils;

public class Warehouse {
    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path",     null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);

        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration config = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // checkpoint
        env.enableCheckpointing(500);
        // 查看flink api 文档，查询对应的类名，看过期的用什么替换
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend");
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // catalog
        WarehouseTableService warehouseTableService = new WarehouseTableService();
        warehouseTableService.createHadoopCatalog(tEnv);

        // 读ncdd_log 表
        // table 转 流
        String logTablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/"+jsonParam.getJson("baseConf").getString("table");
        warehouseTableService.transLogToRecord(env, tEnv, logTablePath, jsonParam);
        env.execute("Warehouse");
    }
}
