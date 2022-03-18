package top.chendaye666.Process;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.NcddLogEntity;

/**
 * 一条日志 展开成多条 table 行
 */
public class WarehouseFlatMap implements FlatMapFunction<NcddLogEntity, String> {
    private static final long serialVersionUID = 3929179607084243642L;
    /*日志解析配置*/
    private final JSONObject sourceType;

    public WarehouseFlatMap(JSONObject sourceType){
        this.sourceType = sourceType;
    }

    @Override
    public void flatMap(NcddLogEntity ncddLogEntity, Collector<String> collector) throws Exception {

        collector.collect(sourceType.getString("gtulog"));
    }
}
