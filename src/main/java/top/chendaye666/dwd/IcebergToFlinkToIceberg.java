package top.chendaye666.dwd;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
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
import top.chendaye666.pojo.NcddztDws;
import top.chendaye666.utils.DateUtil;
import top.chendaye666.utils.RegInxParse;

/**
 * ods_to_dws
 * flink 读取 iceberg 转换 再写到 iceberg
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
    // 从iceberg实时读取数据
    Table table = tEnv.fromDataStream(stream);
    // table 转为 AppendStream 进行处理
    DataStream<Ncddzt> ncddztDataStream = tEnv.toAppendStream(table, Ncddzt.class);
    // ETL
    SingleOutputStreamOperator<NcddztDws> ncddztDwsDataStream =
        ncddztDataStream.process(new ProcessFunction<Ncddzt, NcddztDws>() {
          @Override
          public void open(Configuration parameters) throws Exception {
            // state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
          }

          @Override
          public void processElement(
              Ncddzt ncddzt, Context context, Collector<NcddztDws> collector) throws Exception {
            NcddztDws ncddztDws = new NcddztDws();
            String log = ncddzt.getLog();
            ncddztDws.setSource_type(ncddzt.getSource_type());
            ncddztDws.setIndex(ncddzt.getIndex());
            ncddztDws.setAgent_timestamp(ncddzt.getAgent_timestamp());
            ncddztDws.setIndex(ncddzt.getIndex());
            ncddztDws.setSource_host(ncddzt.getSource_host());
            ncddztDws.setTopic(ncddzt.getTopic());
            ncddztDws.setFile_path(ncddzt.getFile_path());
            ncddztDws.setPosition(ncddzt.getPosition());
            ncddztDws.setTime(DateUtil.formatToTimestamp(
                RegInxParse.matcherValByReg(log, "\\[(\\d{8} \\d{9,})", 1),
                "yyyyMMdd HHmmssSSS"));
            ncddztDws.setLog_type(RegInxParse.matcherValByReg(log, "\\[(send|recv|pub1)\\]", 1));
            ncddztDws.setQd_number(RegInxParse.matcherValByReg(
                log,
                "\\[(send|recv|pub1)\\] \\[(10388101|00102025)\\] \\[" +
                    "(.*?)\\]",
                3));
            ncddztDws.setSeat(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDws.setMarket(RegInxParse.matcherValByReg(log, "\\\"(625|STKBD)\\\":\\\"(\\d{2})\\\"", 2));
            ncddztDws.setCap_acc(RegInxParse.matcherValByReg(log, "(8920|CUACCT_CODE)\\\":\\\"(\\d+)\\\"", 2));
            ncddztDws.setSuborderno(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDws.setWt_pnum(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            ncddztDws.setContract_num(RegInxParse.matcherValByReg(log, "8810\\\":\\\"(\\d{1,})\\\"", 1));
            collector.collect(ncddztDws);
          }
        });
    // .map(NcddztDws::toString).print();

    /*创建临时表*/
    tEnv.createTemporaryView("ods_dws_ncddzt", ncddztDwsDataStream);
    Table dwsTable = tEnv.sqlQuery("select * from ods_dws_ncddzt");

    /*创建DWS表*/
    createDwsTable(tEnv);

    /*Sink*/
    String sinkSql = "INSERT INTO  hadoop_catalog.dws.dws_ncddzt SELECT `source_type`,`index`,`agent_timestamp`," +
        "`source_host`,`topic`,`file_path`,`position`,`time`,`log_type`,`qd_number`,`seat`,`market`,`cap_acc`," +
        "`suborderno`,`wt_pnum`,`contract_num` FROM" +
        " default_catalog" +
        ".default_database" +
        ".ods_dws_ncddzt";
    log.error("sinkSql:\n"+sinkSql);
    tEnv.executeSql(sinkSql);
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
        ") PARTITIONED BY (topic)";
    log.error("dwsNcddztSql=\n" + dwsNcddztSql);
    tEnv.executeSql(dwsNcddztSql);
  }
}
