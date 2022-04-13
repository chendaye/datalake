package top.chendaye666.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import top.chendaye666.Strategy.ParseContext;
import top.chendaye666.Strategy.impl.*;
import top.chendaye666.Strategy.impl.auxiliary.TimestampGtuStrategyImpl;
import top.chendaye666.Strategy.impl.auxiliary.DateGtuStrategyImpl;
import top.chendaye666.Strategy.impl.auxiliary.TimeSpStrategyImpl;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.pojo.ParamEntity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 处理键控流
 */
@Slf4j
public class EtlProcessFunction extends KeyedProcessFunction<String, LogEntity, String> {
    /*日志解析配置*/
    private JSONObject sourceType;
    private ParseContext<LogEntity, String> context;

    public EtlProcessFunction(JSONObject sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 准备工作
        context = new ParseContext<>();
    }

    @Override
    public void processElement(LogEntity value, Context ctx, Collector<String> out) throws Exception {
        HashMap<String, String> keyValueMap = new HashMap<>();
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
        // 遍历每一个字段的解析规则
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            // 要解析的字段
            String logField = next.getKey();
            // 解析字段的方式
            ParamEntity logFieldRule = JSON.toJavaObject((JSONObject)next.getValue(), ParamEntity.class);

            //TODO： 工厂模式 策略模式。 根据不同的解析规则，生成不同的解析器
            switch (logFieldRule.getType()){
                case "index": // 常规正则匹配
                    context.setStrategy(new RegInxStrategyImpl());
                    break;
                case "raw": // 常规取log原始值
                    context.setStrategy(new RawStrategyImpl());
                    break;
                case "raw_log": // 常规取log.log原始值
                    context.setStrategy(new RawLogStrategyImpl());
                    break;
                default:
                    return;
            }
            // 解析值
            String ret = context.executeStrategy(logFieldRule, value);
            if (ret == null){
                //TODO: 没匹配到值
                ret = "";
            }
            // source_type<logField, ret>
            keyValueMap.put(logField, ret);
        }
        keyValueMap.put("source_type", type);
        keyValueMap.put("create_at", System.currentTimeMillis()+"");

        out.collect(JSON.toJSONString(keyValueMap));
    }
}
