package top.chendaye666;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
import top.chendaye666.pojo.NcddztDwd;
import top.chendaye666.util.DateUtil;
import top.chendaye666.util.RegInxParse;


/**
 * ods_to_dwd
 * flink 读取 iceberg 转换 再写到 iceberg
 */
@Slf4j
public class IcebergToFlinkToIcebergDwd {
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
    // 从iceberg实时读取数据
    Table table = tEnv.fromDataStream(stream);
    // table 转为 AppendStream 进行处理
    DataStream<Ncddzt> ncddztDataStream = tEnv.toAppendStream(table, Ncddzt.class);
    // ETL
    SingleOutputStreamOperator<NcddztDwd> ncddztDwsDataStream =
        ncddztDataStream.process(new ProcessFunction<Ncddzt, NcddztDwd>() {
          @Override
          public void open(Configuration parameters) throws Exception {
            // state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
          }

          @Override
          public void processElement(
              Ncddzt ncddzt, Context context, Collector<NcddztDwd> collector) throws Exception {
            NcddztDwd ncddztDwd = new NcddztDwd();
            String log = ncddzt.getLog();
            ncddztDwd.setSource_type(ncddzt.getSource_type());
            ncddztDwd.setIndex(ncddzt.getIndex());
            ncddztDwd.setAgent_timestamp(ncddzt.getAgent_timestamp());
            ncddztDwd.setIndex(ncddzt.getIndex());
            ncddztDwd.setSource_host(ncddzt.getSource_host());
            ncddztDwd.setTopic(ncddzt.getTopic());
            ncddztDwd.setFile_path(ncddzt.getFile_path());
            ncddztDwd.setPosition(ncddzt.getPosition());
            ncddztDwd.setTime(DateUtil.formatToTimestamp(
                RegInxParse.matcherValByReg(log, "\\[(\\d{8} \\d{9,})", 1),
                "yyyyMMdd HHmmssSSS"));
            ncddztDwd.setLog_type(RegInxParse.matcherValByReg(log, "\\[(send|recv|pub1)\\]", 1));
            ncddztDwd.setQd_number(RegInxParse.matcherValByReg(
                log,
                "\\[(send|recv|pub1)\\] \\[(10388101|00102025)\\] \\[" +
                    "(.*?)\\]",
                3));
            ncddztDwd.setSeat(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDwd.setMarket(RegInxParse.matcherValByReg(log, "\\\"(625|STKBD)\\\":\\\"(\\d{2})\\\"", 2));
            ncddztDwd.setCap_acc(RegInxParse.matcherValByReg(log, "(8920|CUACCT_CODE)\\\":\\\"(\\d+)\\\"", 2));
            ncddztDwd.setSuborderno(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDwd.setWt_pnum(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDwd.setContract_num(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            collector.collect(ncddztDwd);
          }
        });
    // .map(NcddztDws::toString).print();

    /*创建临时表*/
    tEnv.createTemporaryView("ods_dwd_ncddzt", ncddztDwsDataStream);
    Table dwdTable = tEnv.sqlQuery("select * from ods_dwd_ncddzt");

    /*创建DWS表*/
    createDwsTable(tEnv);

    /*Sink*/
    String sinkSql = "INSERT INTO  hadoop_catalog.dwd.dwd_ncddzt SELECT `source_type`,`index`,`agent_timestamp`," +
        "`source_host`,`topic`,`file_path`,`position`,`time`,`log_type`,`qd_number`,`seat`,`market`,`cap_acc`," +
        "`suborderno`,`wt_pnum`,`contract_num` FROM" +
        " default_catalog" +
        ".default_database" +
        ".ods_dwd_ncddzt";
    log.error("sinkSql:\n"+sinkSql);
    tEnv.executeSql(sinkSql);
    env.execute();
  }

  /**
   * 创建 dwd 表
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
    tEnv.executeSql("CREATE DATABASE IF NOT EXISTS hadoop_catalog.dwd");
    tEnv.useDatabase("dwd");
    // 建表
    tEnv.executeSql("DROP TABLE IF EXISTS dwd_ncddzt");
    String dwdNcddztSql = "CREATE TABLE  dwd_ncddzt (\n" +
        "   source_type STRING,\n" +
        "   `index` STRING,\n" +
        "   `agent_timestamp` STRING,\n" +
        "   source_host STRING,\n" +
        "   topic STRING,\n" +
        "   file_path STRING,\n" +
        "   `position` STRING,\n" +
        "   `time` BIGINT ,\n" +
        "   log_type String ,\n" +
        "   qd_number String ,\n" +
        "   seat String ,\n" +
        "   market String ,\n" +
        "   cap_acc String ,\n" +
        "   suborderno String ,\n" +
        "   wt_pnum String ,\n" +
        "   contract_num String \n" +
        ") PARTITIONED BY (topic) WITH (\n" +
            "    'write.distribution-mode'='hash'\n" +
            ")";
    log.error("dwdNcddztSql=\n" + dwdNcddztSql);
    tEnv.executeSql(dwdNcddztSql);
  }
}
