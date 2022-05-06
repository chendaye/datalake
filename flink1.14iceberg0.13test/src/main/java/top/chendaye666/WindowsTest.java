package top.chendaye666;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class WindowsTest {
    public static void main(String[] args) throws Exception {
        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // checkpoint
        env.enableCheckpointing(5000);
        // 查看flink api 文档，查询对应的类名，看过期的用什么替换
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend1.14");
        DataStream<Tuple4<String, Integer, Integer, Long>> inputDS = env.addSource(new SourceFunction<Tuple4<String, Integer, Integer, Long>>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Tuple4<String, Integer, Integer, Long>> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(3);
                    int money = random.nextInt(101);
                    //模拟乱序数据
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Tuple4<>(orderId, userId, money, eventTime));
                    TimeUnit.SECONDS.sleep(1);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        inputDS
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple4<String, Integer, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Integer, Integer, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, Integer, Integer, Long> element, long recordTimestamp) {
//                                return element.f3;
                                return System.currentTimeMillis();
                            }
                        })).print("inputOS");

        env.execute("flink-1.14.4");
    }
}
