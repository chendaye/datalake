package top.chendaye666.dwd;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import top.chendaye666.pojo.Ncddzt;

/**
 * ods_to_dws
 * flink 读取 iceberg 转换 再写到
 */
@Slf4j
public class IcebergToFlinkToIceberg {
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
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
    // 准实时的查询
    DataStream<RowData> stream = FlinkSource.forRowData()
        .env(env)
        .tableLoader(tableLoader)
        .streaming(true)
        // .startSnapshotId(2120L)
        .build();
    Table table = tEnv.fromDataStream(stream);
    tEnv.createTemporaryView("ods_ncddzt", stream);

    Table table1 = tEnv.sqlQuery("select * from ods_ncddzt");

    DataStream<Ncddzt> ncddztDataStream = tEnv.toAppendStream(table1, Ncddzt.class);
    ncddztDataStream.process(new ProcessFunction<Ncddzt, Ncddzt>() {
      @Override
      public void processElement(
          Ncddzt ncddzt, Context context, Collector<Ncddzt> collector) throws Exception {

      }
    });
    /*创建DWS表*/
    createDwsTable(tEnv);
    env.execute();
  }

  /**
   * 创建 dws 表
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
    String odsNcddztSql = "CREATE TABLE  dws_ncddzt (\n" +
        "   source_type STRING,\n" +
        "   `index` STRING,\n" +
        "   `agent_timestamp` STRING,\n" +
        "   source_host STRING,\n" +
        "   topic STRING,\n" +
        "   num INT,\n" +
        "   file_path STRING,\n" +
        "   `position` STRING,\n" +
        "   log STRING\n" +
        ")";
    tEnv.executeSql(odsNcddztSql);
  }
}
