package top.chendaye666.controller;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.L5Entity;
import top.chendaye666.service.ClickhouseService;
import top.chendaye666.service.NcddService;
import top.chendaye666.utils.JsonParamUtils;


public class NcddStateController {
    public static NcddService ncddService = new NcddService();
    public static ClickhouseService clickhouseService = new ClickhouseService();

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
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend2"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/" + jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);

        //todo: 计算L5,委托确认耗时(ms): ncddoiw2:BS_NCDD_OIW_COM[channel] - ncddoiw:BS_NCDD_OIW_WT[channel]， match[val_str]

        //todo：方法一： 可以使用 状态存储 之前的流信息

        //todo: 方法二：拆分流，然后join
        SingleOutputStreamOperator<L5Entity> process = ncddService.getL5Stream(stream);
//        top.chendaye666.process.print("1");
//         insert into clickhouse
        clickhouseService.insertIntoClickHouse(process);
        env.execute("stream join");
    }
}
