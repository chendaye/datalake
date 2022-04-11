package top.chendaye666.dao;

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

    /**
     * 创建表
     * @param tEnv
     */
    public void createCommonTable(StreamTableEnvironment tEnv, String tableName){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime."+tableName+" (\n" +
                "    `table_name` STRING,\n" +
                "    `source_type` STRING,\n" +
                "    `mi` STRING,\n" +
                "    `time` STRING,\n" +
                "    `created_at` BIGINT,\n" +
                "    `date` STRING,\n" +
                "    `node` STRING,\n" +
                "    `channel` STRING,\n" +
                "    `channel2` STRING,\n" +
                "    `channel3` STRING,\n" +
                "    `channel4` STRING,\n" +
                "    `channel5` STRING,\n" +
                "    `channel6` STRING,\n" +
                "    `val` FLOAT,\n" +
                "    `val_str` STRING\n" +
                ") PARTITIONED BY (`date`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";
        tEnv.executeSql(sql);
    }

    /**
     * ncdd_log 表
     * @param env
     * @param tEnv
     * @return
     */
    public Table getNcddLog(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String logTablePath){
        // 读ncdd_log 表
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable(logTablePath);
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                // .startSnapshotId(2120L)
                .build();
//         stream.print();
        Table table = tEnv.fromDataStream(stream,
                $("date"),
                $("log")
        );
        return table;
    }
}
