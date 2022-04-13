package top.chendaye666;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.Service.EtlLogService;
import top.chendaye666.Service.EtlToTableervice;
import top.chendaye666.pojo.LogEntity;
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
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // etl log
        EtlLogService etlLogService = new EtlLogService();
        SingleOutputStreamOperator<String> etl = etlLogService.etl(jsonParam, env);
        etl.print("etl");

        // insert table
        EtlToTableervice etlToTableervice = new EtlToTableervice();
        etlToTableervice.createHadoopCatalog(tEnv);
        String tableName = jsonParam.getJson("baseConf").getString("table");
        etlToTableervice.createOdsTable(tEnv, "hadoop_prod.realtime."+tableName);
        etlToTableervice.insert(tEnv, etl, "hadoop_prod.realtime."+tableName);

        env.execute("ETL");
    }


}
