package top.chendaye666.controller;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.ResultEntity;
import top.chendaye666.process.ClickHouseJoinSink;
import top.chendaye666.service.ClickhouseService;
import top.chendaye666.service.NcddService;
import top.chendaye666.utils.JsonParamUtils;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Random;

/**
 * 使用流join 匹配，求时间差
 */
public class NcddJoinController {
    public static NcddService ncddService = new NcddService();

    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path", null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);

        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration config = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend");
        env.getCheckpointConfig().setCheckpointTimeout(60000 * 2);
        // tEnv 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // iceberg table data
        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/" + jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);
        // join 结果
        DataStream<ResultEntity> joinL5Stream = ncddService.joinL5Stream(stream);
        // sink clickhouse
        ClickhouseService clickhouseService = new ClickhouseService();
        // 建表
        clickhouseService.createL5Table();
        clickhouseService.insertIntoClickHouse2(joinL5Stream);
        env.execute("stream join");
    }
}
