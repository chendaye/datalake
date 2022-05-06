package top.chendaye666.controller;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.process.OrdernessGenerator;
import top.chendaye666.service.NcddService;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Random;

public class NcddWindowTestController {
    public static NcddService ncddService = new NcddService();

    public static void main(String[] args) throws Exception {
        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 开启本地环境 webui：localhouse:8081
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(3);
        // checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend");
        env.getCheckpointConfig().setCheckpointTimeout(60000*2);



        ExecutionConfig executionConfig = env.getConfig();


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general";
//        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);
        DataStream<Row> rowStream = ncddService.getRowStream(env, tEnv, tablePath);

        rowStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Row>() {
                    @Override
                    public long extractTimestamp(Row element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                })).setParallelism(2)
                .print("rowstream");


        // 设置时间戳和水印
//        SingleOutputStreamOperator<CommonTableEntity> streamOperator = stream
//                .filter(new FilterFunction<CommonTableEntity>() {
//                    @Override
//                    public boolean filter(CommonTableEntity value) throws Exception {
//                        return value.getSource_type().equals("ncddoiw") || value.getSource_type().equals("ncddoiw2");
//                    }
//                })
//                .map(new MapFunction<CommonTableEntity, CommonTableEntity>() {
//                    @Override
//                    public CommonTableEntity map(CommonTableEntity value) throws Exception {
//                        //DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                        //long eventTime = dateFormat.parse(value.getChannel() + "", new ParsePosition(0)).getTime();
//                        //模拟乱序数据
//                        Random random = new Random();
//                        long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
//                        value.setCreated_at(eventTime);
//                        return value;
//                    }
//                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
//                            @Override
//                            public long extractTimestamp(CommonTableEntity element, long recordTimestamp) {
//                                Random random = new Random();
//                                long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
//                                return eventTime;
//                            }
//                        }));
//        streamOperator.print("watermark");


//        streamOperator
//                .keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
//                    @Override
//                    public String getKey(Tuple3<String, String, Long> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .sum("f2")
//                .print("wtf");
        env.execute("wtf");
    }
}
