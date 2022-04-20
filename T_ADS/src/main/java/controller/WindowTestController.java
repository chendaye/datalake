package controller;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import utils.JsonParamUtils;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;

public class WindowTestController {
    public static void main(String[] args) throws Exception {

        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "test-window");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("windows_test2", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始,默认从最新开始

        SingleOutputStreamOperator<String> process = env.addSource(consumer)
                .map(new MapFunction<String, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(String s) throws Exception {
                        String[] s1 = s.split(" ");
                        return new Tuple3<>(s1[0], Long.parseLong(s1[1]), Long.parseLong(s1[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofMillis(300))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Long> stringLongLongTuple3, long l) {
                                return stringLongLongTuple3.f2;
                            }
                        }))
                .keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Long> s) throws Exception {
                        return s.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .process(new ProcessWindowFunction<Tuple3<String, Long, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<String> out) throws Exception {
                        Iterator<Tuple3<String, Long, Long>> iterator = elements.iterator();
                        long sum = 0L;
                        while (iterator.hasNext()){
                            Tuple3<String, Long, Long> next = iterator.next();
                            sum = sum + next.f1;
                        }
                        Tuple3<String, Long, Long> next = iterator.next();
                        out.collect(context.window().getEnd()+" "+ sum);
                    }
                });
        process.print();

        env.execute("test");
    }
}
