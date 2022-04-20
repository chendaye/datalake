package controller;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import pojo.CommonTableEntity;
import service.NcddService;
import utils.JsonParamUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class NcddWindowTestController {
    public static NcddService ncddService = new NcddService();

    public static void main(String[] args) throws Exception {
        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general";
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);
        // 测试滚动窗口
        stream
                .map(new MapFunction<CommonTableEntity, CommonTableEntity>() {
                    @Override
                    public CommonTableEntity map(CommonTableEntity commonTableEntity) throws Exception {
                        int num = 1 + (int) (Math.random() * (10 - 1 + 1));
                        commonTableEntity.setCreated_at(System.currentTimeMillis() + num);
                        return commonTableEntity;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
                            @Override
                            public long extractTimestamp(CommonTableEntity commonTableEntity, long l) {
//                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                        long timestamp = dateFormat.parse(commonTableEntity.getCreated_at()+"", new ParsePosition(0)).getTime();
//                        System.out.println(commonTableEntity.getTime());
                                return commonTableEntity.getCreated_at();
                            }
                        }))

                .keyBy(new KeySelector<CommonTableEntity, String>() {
                    @Override
                    public String getKey(CommonTableEntity commonTableEntity) throws Exception {
                        return commonTableEntity.getNode();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ProcessWindowFunction<CommonTableEntity, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<CommonTableEntity> elements, Collector<String> out) throws Exception {
                        CommonTableEntity next = elements.iterator().next();
                        System.out.println(next.toString());
                        out.collect(next.getVal_str());
                    }
                })
                .print("wtf");


        env.execute("test");
    }
}
