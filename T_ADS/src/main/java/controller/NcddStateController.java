package controller;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import pojo.CommonTableEntity;
import pojo.L5Entity;
import service.ClickhouseService;
import service.NcddService;
import utils.JsonParamUtils;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;


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
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend2"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/" + jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);

        //todo: 计算L5,委托确认耗时(ms): ncddoiw2:BS_NCDD_OIW_COM[channel] - ncddoiw:BS_NCDD_OIW_WT[channel]， match[val_str]

        //todo：方法一： 可以使用 状态存储 之前的流信息

        //todo: 方法二：拆分流，然后join
        SingleOutputStreamOperator<L5Entity> process = ncddService.getL5Stream(stream);

        // insert into clickhouse
        clickhouseService.insertIntoClickHouse(env, process);
        env.execute("stream join");
    }
}
