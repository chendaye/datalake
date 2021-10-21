package top.chendaye666;

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
 *
 * 在表中定义事件时间
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/streaming/time_attributes.html
 */
@Slf4j
public class IcebergToFlinkToIcebergDws {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint
    env.enableCheckpointing(5000);
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));

    EnvironmentSettings blinkStreamSettings =
        EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
    System.setProperty("HADOOP_USER_NAME", "root");

    // table 转 流
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/dwd/dwd_ncddzt");
    // batch 查询
    DataStream<RowData> stream = FlinkSource.forRowData()
        .env(env)
        .tableLoader(tableLoader)
        .streaming(false)
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
    tEnv.createTemporaryView("ods_dws_ncddzt", table);
//    Table table1 = tEnv.sqlQuery("select `source_type`,`agent_timestamp`,topic,SUM(`time`) as total from ods_dws_ncddzt " +
//            "group by `source_type`,`agent_timestamp`,topic");
//    TableResult execute = table1.execute();
//    CloseableIterator<Row> collect = execute.collect();
//    DataStream<Tuple2<Boolean, NcddztDws>> tuple2DataStream = tEnv.toRetractStream(table1, NcddztDws.class);
//    tuple2DataStream.map(v -> v.f1.toString()).print();
//    DataStream<NcddztDws> ncddztDwsDataStream = tEnv.toAppendStream(table1, NcddztDws.class);


    /*创建DWS表*/
     createDwsTable(tEnv);

    /*Sink*/
    String sinkSql = "INSERT INTO  hadoop_catalog.dws.dws_ncddzt SELECT `source_type`,`agent_timestamp`," +
            "topic,SUM(`time`) as total FROM " +
        " default_catalog" +
        ".default_database" +
        ".ods_dws_ncddzt group by `source_type`,`agent_timestamp`,topic";
    log.error("sinkSql:\n"+sinkSql);
     tEnv.executeSql(sinkSql);
//    env.execute();
  }

  /**
   * 创建 dws 表
   *
   * @param tEnv
   */
  public static void createDwsTable(StreamTableEnvironment tEnv) {
    String hadoopCatalogSql = "CREATE CATALOG hadoop_catalog WITH (\n" +
        "  'type'='iceberg',\n" +
        "  'catalog-type'='hadoop',\n" +
        "  'warehouse'='hdfs://hadoop01:8020/warehouse/path',\n" +
        "  'property-version'='1'\n" +
        ")";
    tEnv.executeSql(hadoopCatalogSql);
    // use catalog
    tEnv.useCatalog("hadoop_catalog");
    // 建数据库
    tEnv.executeSql("CREATE DATABASE IF NOT EXISTS hadoop_catalog.dws");
    tEnv.useDatabase("dws");
    // 建表
    tEnv.executeSql("DROP TABLE IF EXISTS dws_ncddzt");
    String dwsNcddztSql = "CREATE TABLE  dws_ncddzt (\n" +
        "   source_type STRING,\n" +
        "   `agent_timestamp` STRING,\n" +
        "   topic STRING,\n" +
        "   total BIGINT \n" +
        ") PARTITIONED BY (topic)";
    log.error("dwsNcddztSql=\n" + dwsNcddztSql);
    tEnv.executeSql(dwsNcddztSql);
  }


}
