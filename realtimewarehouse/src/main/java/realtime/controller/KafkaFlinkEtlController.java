package realtime.controller;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * ETL 数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/connectors/kafka.html
 */
public class KafkaFlinkEtlController {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        properties.setProperty("group.id", "ncdd");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("ncdd_log", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
        DataStream<String> stream = env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        stream.print();
        env.execute("ETL");
    }
}
