package top.chendaye666.dao;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
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
         Table table = tEnv.fromDataStream(stream,
                Schema.newBuilder()
//                        .column("date", "STRING")
//                        .column("log", "STRING")
                        .column("table_name", "STRING")
                        .column("source_type", "STRING")
                        .column("mi", "STRING")
                        .column("time", "STRING")
                        .column("date", "STRING")
                        .column("created_at", "BIGINT")
                        .column("node", "STRING")
                        .column("channel", "STRING")
                        .column("channel2", "STRING")
                        .column("channel3", "STRING")
                        .column("channel4", "STRING")
                        .column("channel5", "STRING")
                        .column("channel6", "STRING")
                        .column("val", "FLOAT")
                        .column("val_str", "STRING")
//                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(created_at, 3)")
//                        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                        .build());

        return table;
    }
}
