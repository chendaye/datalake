package top.chendaye666.v2;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

import static org.apache.flink.table.api.Expressions.$;


/**
 * ods_to_dwd
 * flink 读取 iceberg 转换 再写到 iceberg
 * /opt/flink-1.12.5/bin/flink run -t yarn-per-job --detached  /opt/work/datalake/dwd-1.0-SNAPSHOT.jar
 */
@Slf4j
public class IcebergToFlinkToSparkIcebergDwd {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/ods/ncddzt");
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                // .startSnapshotId(2120L)
                .build();
        // stream.print();

        // 从iceberg实时读取数据
        Table table = tEnv.fromDataStream(stream,
                $("source_type"),
                $("index"),
                $("agent_timestamp"),
                $("topic"),
                $("file_path"),
                $("position"),
                $("source_host"),
                $("log"),
                $("num")
        );

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
//        ncddztDwsDataStream.map(NcddztDwd::toString).print();

        /*创建临时表*/
        tEnv.createTemporaryView("ods_ncddzt", ncddztDwsDataStream);
        Table dwdTable = tEnv.sqlQuery("select * from ods_ncddzt");

        /*创建DWS表*/
        /*createDwsTable(tEnv);*/

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
        String sinkSql = "INSERT INTO  hadoop_prod.dwd.ncddzt SELECT `source_type`,`index`,`agent_timestamp`," +
                "`source_host`,`topic`,`file_path`,`position`,`time`,`log_type`,`qd_number`,`seat`,`market`,`cap_acc`," +
                "`suborderno`,`wt_pnum`,`contract_num` FROM" +
                " default_catalog" +
                ".default_database" +
                ".ods_ncddzt";
        log.error("sinkSql:\n" + sinkSql);
        tEnv.executeSql(sinkSql);
//        env.execute("dwd-iceberg");
    }

}
