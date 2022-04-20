package dao;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 获取表
 */
public class NcddDao {

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
     * ncdd_log 表
     * @param env
     * @param tEnv
     * @return
     */
    public Table getNcddGtu(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String logTablePath, boolean isStream){
        // 读ncdd_common 表
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable(logTablePath);
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(isStream )
                // .startSnapshotId(2120L)
                .build();
//         stream.print("RowData");
//
         Table table = tEnv.fromDataStream(stream,
                $("source_type"),
                $("mi"),
                $("time"),
                $("created_at"),
                $("date"),
                $("node"),
                $("channel"),
                $("channel2"),
                $("channel3"),
                $("channel4"),
                $("channel5"),
                $("channel6"),
                $("val"),
                $("val_str")
        );
        return table;
    }
}
