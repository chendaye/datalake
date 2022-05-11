package top.chendaye666;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import java.time.Duration;
import java.util.Random;

public class ReadTest {
    public static void main(String[] args) throws Exception {
        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // checkpoint
        env.enableCheckpointing(5000);
        // 查看flink api 文档，查询对应的类名，看过期的用什么替换
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend1.14");

//        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String hadoopCatalogSql = "CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.executeSql(hadoopCatalogSql);

        String logTablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general";

        TableLoader tableLoader = TableLoader.fromHadoopTable(logTablePath);
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource
                .forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
//                .startSnapshotId(3615347613201348772L)
                .build();
        stream.print("wtf");
        env.execute("flink-1.14.4");
    }
}
