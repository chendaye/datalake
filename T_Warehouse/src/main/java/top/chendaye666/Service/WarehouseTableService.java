package top.chendaye666.Service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.Process.WarehouseFlatMap;
import top.chendaye666.Process.WarehouseProcessFunction;
import top.chendaye666.dao.NcddDao;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.NcddLogEntity;
import top.chendaye666.utils.JsonParamUtils;

import java.util.HashSet;
import java.util.Iterator;

public class WarehouseTableService {

    /**
     * catalog
     * @param tEnv
     */
    public void createHadoopCatalog(StreamTableEnvironment tEnv) {
        String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.executeSql(hadoopCatalogSql);
    }

    /**
     * 入库
     * @param env
     * @param tEnv
     * @param logTablePath
     * @param jsonParam
     */
    public void transLogToRecord(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String logTablePath, JsonParamUtils jsonParam){
        // tables set
        HashSet<String> tableSet = new HashSet<>();
        tableSet = jsonParam.getTableSet();

        NcddDao ncddDao = new NcddDao();
        Table ncddLog = ncddDao.getNcddLog(env, tEnv, logTablePath);

        // table 转为 AppendStream 进行处理
        SingleOutputStreamOperator<CommonTableEntity> sourceType = tEnv.toAppendStream(ncddLog, NcddLogEntity.class)
                .flatMap(new WarehouseFlatMap(jsonParam.getJson("sourceType")));
        // 每个表分别插入数据
        Iterator<String> iterator = tableSet.iterator();
        while (iterator.hasNext()){
            String tableName = iterator.next();
            SingleOutputStreamOperator<CommonTableEntity> tableStream = sourceType.filter(new FilterFunction<CommonTableEntity>() {
                @Override
                public boolean filter(CommonTableEntity commonTableEntity) throws Exception {
                    return commonTableEntity.getTable_name().equals(tableName);
                }
            });
            Table table = tEnv.fromDataStream(tableStream);

        }



    }
}
