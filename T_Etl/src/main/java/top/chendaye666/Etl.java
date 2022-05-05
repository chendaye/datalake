package top.chendaye666;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.Service.EtlLogService;
import top.chendaye666.Service.EtlToTableervice;
import top.chendaye666.utils.JsonParamUtils;


/**
 * ETL 数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/connectors/kafka.html
 */
public class Etl {
    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path", null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);
        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(500);
        // 查看flink api 文档，查询对应的类名，看过期的用什么替换
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend");
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // etl log
        EtlLogService etlLogService = new EtlLogService();
        SingleOutputStreamOperator<String> etl = etlLogService.etl(jsonParam, env);

        // insert table
        EtlToTableervice etlToTableervice = new EtlToTableervice();
        etlToTableervice.createHadoopCatalog(tEnv);
        String tableName = "hadoop_prod.realtime."+jsonParam.getJson("baseConf").getString("table");
        etlToTableervice.createOdsTable(tEnv, tableName);
        etlToTableervice.insert(tEnv, etl, tableName);

        env.execute("ETL");
    }


}
