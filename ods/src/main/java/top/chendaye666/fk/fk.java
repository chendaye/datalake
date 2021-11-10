package top.chendaye666.fk;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Properties;

/**
 * ods：Kafka 数据入 Iceberg
 * /opt/flink-1.12.5/bin/flink run -t yarn-per-job --detached  /opt/work/datalake/ods-1.0-SNAPSHOT.jar
 */
@Slf4j
public class fk {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置 kafaka参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092");
        props.put("zookeeper.connect", "hadoop01:2181");
        //props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("ods_ncddzt", new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        // 添加数据源
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
        env.execute("f-------------------------------k!");
    }
}
