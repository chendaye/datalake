package top.chendaye666.v2;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;

/**
 * ods_to_dws
 * flink 读取 iceberg 转换 再写到 iceberg
 * <p>
 * 在表中定义事件时间
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/time_attributes.html
 */
@Slf4j
public class IcebergToFlinkToSparkIcebergDws {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/dwd/ncddzt");
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

        //todo: 创建 hadoop catalog
        String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        log.info("iceberg hadoop_prod:\n" + hadoopCatalogSql);
        tEnv.executeSql(hadoopCatalogSql);

        /*Sink*/
        String sinkSql = "INSERT INTO  hadoop_prod.dws.ncddzt SELECT `source_type`,`agent_timestamp`," +
                "topic,SUM(`time`) as total FROM " +
                " default_catalog" +
                ".default_database" +
                ".dws_ncddzt group by `source_type`,`agent_timestamp`,topic";
        log.error("这里运行的是DWS:\n" + sinkSql + "\n\n DWS DWS DWS DWS DWS DWS DWS DWS DWS DWS DWS \n\n ");
        tEnv.executeSql(sinkSql);
    }


}
