package top.chendaye666.Service;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import top.chendaye666.Process.WarehouseFlatMap;
import top.chendaye666.Process.WarehouseProcessFunction;
import top.chendaye666.dao.NcddDao;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.NcddLogEntity;
import top.chendaye666.pojo.RecordEntity;
import top.chendaye666.utils.JsonParamUtils;
import static org.apache.flink.table.api.Expressions.$;

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
     * 入库: hdfs dfs -ls /warehouse/iceberg/realtime/ncdd_common/data/date=2022-03-22
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
        SingleOutputStreamOperator<CommonTableEntity> commonTableEntityStream = tEnv.toAppendStream(ncddLog, NcddLogEntity.class)
                .flatMap(new WarehouseFlatMap(jsonParam.getJson("sourceType")));

//        SingleOutputStreamOperator<RecordEntity> recordEntityStream = commonTableEntityStream.process(new WarehouseProcessFunction());

        // 每个表分别插入数据
        Iterator<String> iterator = tableSet.iterator();
        // 遍历每一张表
//        while (iterator.hasNext()){
            String tableName = iterator.next();
            // 过滤流
//            SingleOutputStreamOperator<CommonTableEntity> splitStream = commonTableEntityStream.filter(new FilterFunction<CommonTableEntity>() {
//                @Override
//                public boolean filter(CommonTableEntity commonTableEntity) throws Exception {
//                    return commonTableEntity.getTable_name().equals(tableName);
//                }
//            });
            //拆分流
//            DataStream<RecordEntity> sideOutput = recordEntityStream.getSideOutput(new OutputTag<RecordEntity>(tableName) {});

            // 插入对应的表
            String tagTable = "tag_"+tableName;
            tEnv.createTemporaryView(tagTable, commonTableEntityStream);
            ncddDao.createCommonTable(tEnv, tableName);
            String sinkSql = "INSERT INTO  hadoop_prod.realtime."+tableName+" SELECT " +
                    "`table_name`, " +
                    "`source_type`, " +
                    "`mi`, " +
                    "`time`, " +
                    "`created_at`, " +
                    "`date`, " +
                    "`node`, " +
                    "`channel`, " +
                    "`channel2`, " +
                    "`channel3`, " +
                    "`channel4`, " +
                    "`channel5`, " +
                    "`channel6`, " +
                    "`val`, " +
                    "`val_str` " +
                    "from default_catalog.default_database."+tagTable ;
        tEnv.executeSql(sinkSql);
//        }
    }
}
