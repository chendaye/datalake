package top.chendaye666.controller;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.chendaye666.process.Splitter;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CustomSourceController {
    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(3);
        // 自定义Source
        DataStream<Tuple4<String, Integer, Integer, Long>> inputDS = env.addSource(new SourceFunction<Tuple4<String, Integer, Integer, Long>>() {
            private boolean flag = true;
            @Override
            public void run(SourceContext<Tuple4<String, Integer, Integer, Long>> ctx) throws Exception {
                Random random = new Random();
                while (flag){
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(101);
                    //模拟乱序数据
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Tuple4<>(orderId,userId,money,eventTime));
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });


//       inputDS.print("OS");
        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Long>> tuple4SingleOutputStreamOperator = inputDS
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple4<String, Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Integer, Integer, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, Integer, Integer, Long> element, long recordTimestamp) {
//                                return element.f3;
                                return System.currentTimeMillis();
                            }
                        }));
        tuple4SingleOutputStreamOperator.print("watermark");
//        tuple4SingleOutputStreamOperator
//                .keyBy(new KeySelector<Tuple4<String, Integer, Integer, Long>, Integer>() {
//                    @Override
//                    public Integer getKey(Tuple4<String, Integer, Integer, Long> value) throws Exception {
//                        return value.f1;
//                    }
//                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .sum("f2")
//                .print("wtf");
        env.execute("Window WordCount");
    }
}
