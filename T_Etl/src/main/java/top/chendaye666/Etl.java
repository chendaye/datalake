package top.chendaye666;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.process.EtlProcessFunction;
import top.chendaye666.utils.JsonParamUtils;

import java.io.File;
import java.util.Properties;

/**
 * ETL 数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/connectors/kafka.html
 */
public class Etl {
    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path", null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);
        // flink 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", jsonParam.getJson("baseConf").getString("kafkaAdds"));
        properties.setProperty("group.id", jsonParam.getJson("baseConf").getString("consumerID"));
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(jsonParam.getJson("baseConf").getString("topicName"), new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
        // 日志转化为实体类
        SingleOutputStreamOperator<String> sourceTypeContentStream = env.addSource(consumer).map(new MapFunction<String, LogEntity>() {
            @Override
            public LogEntity map(String s) throws Exception {
                return JSONObject.parseObject(s, LogEntity.class);
            }
        })
                .keyBy(LogEntity::getSource_type)
                .process(new EtlProcessFunction(jsonParam.getJson("sourceType")));

        sourceTypeContentStream.print();
        env.execute("ETL");
    }
}
