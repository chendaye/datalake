package top.chendaye666.Process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.calcite.util.ReflectUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.MisEntity;
import top.chendaye666.pojo.NcddLogEntity;
import top.chendaye666.reflect.ReflectUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * 一条日志 展开成多条 table 行
 */
public class WarehouseFlatMap implements FlatMapFunction<NcddLogEntity, CommonTableEntity> {
    private static final long serialVersionUID = 3929179607084243642L;
    /*日志解析配置*/
    private final JSONObject sourceType;

    public WarehouseFlatMap(JSONObject sourceType){
        this.sourceType = sourceType;
    }

    @Override
    public void flatMap(NcddLogEntity ncddLogEntity, Collector<CommonTableEntity> collector) throws Exception {
        // log
        JSONObject ncddLog = JSON.parseObject(ncddLogEntity.getLog());
        String date = ncddLogEntity.getDate();
        // sourceType
        String sourceTypeName = ncddLog.getString("source_type");
        JSONObject sourceTypeCurrent = sourceType.getJSONObject(sourceTypeName);
        String tableName = sourceTypeCurrent.getString("tableName");
        JSONArray misArray = sourceTypeCurrent.getJSONArray("mis");
        // map 值
        Iterator<Object> misIterator = misArray.iterator();
        while (misIterator.hasNext()){
            JSONObject mis = (JSONObject)misIterator.next();
            // misMap
            Iterator<Map.Entry<String, Object>> misMapIterator = mis.entrySet().iterator();
            // 一条table 记录
            CommonTableEntity tableEntity = new CommonTableEntity();
            JSONObject tableEntityJson = (JSONObject)JSONObject.toJSON(tableEntity);
            tableEntityJson.put("date", date);
            tableEntityJson.put("created_at", System.currentTimeMillis());
            tableEntityJson.put("table_name", tableName);
            tableEntityJson.put("source_type", sourceTypeName);
            while (misMapIterator.hasNext()){
                Map.Entry<String, Object> next = misMapIterator.next();
                String field = next.getKey();
                MisEntity misEntity = JSON.toJavaObject((JSONObject) next.getValue(), MisEntity.class);
                String fieldVal;
                switch (misEntity.getType()){
                    case "raw" :
                        fieldVal = misEntity.getValue();
                        break;
                    case "map" :
                        fieldVal = ncddLog.getString(misEntity.getField());
                        break;
                    default:
                        fieldVal = null;
                        break;
                }
                // 入库日期
                tableEntityJson.put(field, fieldVal);
            }

            // 每一个 mis 生成一条记录（SerializerFeature.WriteMapNullValue 保留 null 值）
//            collector.collect(JSONObject.toJSONString(tableEntityJson, SerializerFeature.WriteMapNullValue));
            collector.collect(tableEntityJson.toJavaObject(CommonTableEntity.class));
        }
    }
}
