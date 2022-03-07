package top.chendaye666.process;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBiMap;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
import top.chendaye666.pojo.LogEntity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 处理键控流
 */
@Slf4j
public class EtlProcessFunction extends KeyedProcessFunction<String, LogEntity, String>{
    /*日志解析配置*/
    private JSONObject sourceType;

    public EtlProcessFunction(JSONObject sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 准备工作
    }

    @Override
    public void processElement(LogEntity value, Context ctx, Collector<String> out) throws Exception {
        // 当前log 的 source_type 类型
        String type = value.getSource_type();
        // 当前 source_type 对应的解析规则
        JSONObject parseRules = sourceType.getJSONObject(type);
        if (parseRules == null || parseRules.toString() == null){
            log.info("source_type 类型："+type+",没有定义解析规则！");
            return;
        }
        // 保存解析的日志数据
        Iterator<Map.Entry<String, Object>> iterator = parseRules.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            // 要解析的字段
            String logField = next.getKey();
            // 解析字段的方式
            JSONObject logFieldRule = (JSONObject)next.getValue();
            System.out.println(logField+"----------"+logFieldRule+"-------"+logFieldRule.getString("type"));
            //TODO： 工厂模式 策略模式。 根据不同的解析规则，生成不同的解析器

        }
        HashMap<String, String> keyValueMap = new HashMap<>();
        out.collect("");
    }
}
