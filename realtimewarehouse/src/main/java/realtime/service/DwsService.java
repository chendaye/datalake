package realtime.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;


import static org.apache.flink.table.api.Expressions.$;

public class DwsService {
    public void createDwsTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.dws_ncddzt (\n" +
                "    `source_type` STRING,\n" +
                "    `agent_timestamp` STRING,\n" +
                "    `topic` STRING,\n" +
                "    `total` BIGINT\n" +
                ") PARTITIONED BY (`topic`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }

    public void insertToDws(StreamExecutionEnvironment env, StreamTableEnvironment tEnv){
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/realtime/dwd_ncddzt");
        // batch 查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();
        // 从iceberg 批量读取所有数据
        Table table = tEnv.fromDataStream(stream,
                $("source_type"),
                $("index"),
                $("agent_timestamp"),
                $("source_host"),
                $("topic"),
                $("file_path"),
                $("position"),
                $("time"),
                $("log_type"),
                $("qd_number"),
                $("seat"),
                $("market"),
                $("cap_acc"),
                $("suborderno"),
                $("wt_pnum"),
                $("contract_num")
        );
        // 创建临时表
        tEnv.createTemporaryView("dws_ncddzt", table);

        /*Sink*/
        String sinkSql = "INSERT INTO  hadoop_prod.realtime.dws_ncddzt SELECT `source_type`,`agent_timestamp`," +
                "topic,SUM(`time`) as total FROM " +
                " default_catalog" +
                ".default_database" +
                ".dws_ncddzt group by `source_type`,`agent_timestamp`,topic";
        tEnv.executeSql(sinkSql);
    }
}
