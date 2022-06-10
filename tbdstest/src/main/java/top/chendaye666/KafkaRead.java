package top.chendaye666;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(60 * 1000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hdfsCluster/user/chenxiaolong1/flink-checkpoints");
        env.getCheckpointConfig().setCheckpointTimeout(60000 * 2);

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.60.6.187:9092,10.60.6.188:9092,10.60.6.189:9092,10.60.6.179:9092,10.60.6.180:9092,10.60.6.181:9092");
        props.put("group.id", "tbds");
        props.put("auto.offset.reset", "latest"); // latest/earliest
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "3000");

        // TBDS原生认证参数设置
        props.put("security.protocol", "SASL_TBDS");
        props.put("sasl.mechanism", "TBDS");
        props.put("sasl.tbds.secure.id", "z87A4rgCyxKQsxl1HMddJzqvnDE9qDcHn5qu");
        props.put("sasl.tbds.secure.key", "ZQP4suI4RvIittcng5aaNhsRh0Bo6V77");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("otcsys", new SimpleStringSchema(), props);
        DataStreamSource<String> source = env.addSource(consumer);
        source.print();
        env.execute("测试kafka连通");
    }
}
