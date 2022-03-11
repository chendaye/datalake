package top.chendaye666.Service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.process.EtlProcessFunction;
import top.chendaye666.utils.JsonParamUtils;

import java.util.Properties;

/**
 * 日志清洗
 */
public class EtlLog {
    public SingleOutputStreamOperator<String> etl(JsonParamUtils jsonParam, StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", jsonParam.getJson("baseConf").getString("kafkaAdds"));
        properties.setProperty("group.id", jsonParam.getJson("baseConf").getString("consumerID"));
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(jsonParam.getJson("baseConf").getString("topicName"), new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
//        env.addSource(consumer).print();
        // 日志转化为实体类
        SingleOutputStreamOperator<String> sourceTypeContentStream = env.addSource(consumer).map(new MapFunction<String, LogEntity>() {
            @Override
            public LogEntity map(String s) throws Exception {
                return JSONObject.parseObject(s, LogEntity.class);
            }
        })
                .keyBy(LogEntity::getSource_type)
                .process(new EtlProcessFunction(jsonParam.getJson("sourceType")));
        // TODO:写入表
        sourceTypeContentStream.print();
        return sourceTypeContentStream;
    }
}
