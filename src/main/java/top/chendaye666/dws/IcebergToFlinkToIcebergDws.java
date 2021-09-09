package top.chendaye666.dws;

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
import top.chendaye666.pojo.NcddztDwd;
import org.apache.flink.table.api.Expressions;

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
            Expressions.$("source_type"),
            Expressions.$("index"),
            Expressions.$("agent_timestamp"),
            Expressions.$("source_host"),
            Expressions.$("topic"),
            Expressions.$("file_path"),
            Expressions.$("position"),
            Expressions.$("time"),
            Expressions.$("log_type"),
            Expressions.$("qd_number"),
            Expressions.$("seat"),
            Expressions.$("market"),
            Expressions.$("cap_acc"),
            Expressions.$("suborderno"),
            Expressions.$("wt_pnum"),
            Expressions.$("contract_num")
    );
    // 创建临时表
    tEnv.createTemporaryView("ods_dws_ncddzt", table);
    Table table1 = tEnv.sqlQuery("select * from ods_dws_ncddzt");
    tEnv.toAppendStream(table1, NcddztDwd.class).map(NcddztDwd::toString).print();



    /*创建DWS表*/
    // createDwsTable(tEnv);

    /*Sink*/
    String sinkSql = "INSERT INTO  hadoop_catalog.dws.dws_ncddzt SELECT `source_type`,`index`,`agent_timestamp`," +
        "`topic`,`time`,`sum` FROM" +
        " default_catalog" +
        ".default_database" +
        ".ods_dws_ncddzt";
    log.error("sinkSql:\n"+sinkSql);
    // tEnv.executeSql(sinkSql);
    env.execute();
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
        "   `index` STRING,\n" +
        "   `agent_timestamp` STRING,\n" +
        "   topic STRING,\n" +
        "   `time` BIGINT ,\n" +
        "   sum BIGINT \n" +
        ") PARTITIONED BY (topic)";
    log.error("dwsNcddztSql=\n" + dwsNcddztSql);
    tEnv.executeSql(dwsNcddztSql);
  }
}
