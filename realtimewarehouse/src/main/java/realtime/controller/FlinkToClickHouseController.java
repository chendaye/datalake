package realtime.controller;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.dao.NcddztDws;
import realtime.service.ClickhouseService;
import realtime.service.CommonService;
import realtime.util.ClickHouseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * flink 写入 clickhouse
 * /opt/flink-1.12.5/bin/flink run -m yarn-cluster -ynm clickhouse -c realtime.top.chendaye666.controller.FlinkToClickHouseController  -yjm 2048 -ytm 1024 -d /opt/work/realtimewarehouse-1.0-SNAPSHOT.jar
 */
public class FlinkToClickHouseController {
    public static CommonService commonService = new CommonService();
    public static ClickhouseService clickhouseService = new ClickhouseService();

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);
        // insert clickhouse
        DataStream<NcddztDws> streamFromDws = clickhouseService.getStreamFromDws(env, tEnv);
        clickhouseService.insertIntoClickHouse(env, streamFromDws);
    }
}
