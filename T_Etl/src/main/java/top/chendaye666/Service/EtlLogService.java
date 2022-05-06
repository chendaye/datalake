package top.chendaye666.Service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.process.EtlKeyedProcessFunction;
import top.chendaye666.process.EtlProcessFunction;
import top.chendaye666.utils.JsonParamUtils;

/**
 * 日志清洗
 */
public class EtlLogService {
    public SingleOutputStreamOperator<String> etl(JsonParamUtils jsonParam, StreamExecutionEnvironment env) {

        // https://blog.csdn.net/xianpanjia4616/article/details/120735539
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(jsonParam.getJson("baseConf").getString("kafkaAdds"))
                .setTopics(jsonParam.getJson("baseConf").getString("topicName"))
                .setGroupId(jsonParam.getJson("baseConf").getString("consumerID"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 日志转化为实体类
        SingleOutputStreamOperator<String> sourceTypeContentStream = kafkaSource
                .process(new EtlProcessFunction(jsonParam.getJson("sourceType")));
        return sourceTypeContentStream;
    }
}

