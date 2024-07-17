package top.chendaye666;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

        String logTablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/rwtest6";
//
        TableLoader tableLoader = TableLoader.fromHadoopTable(logTablePath);
//        long snapshotId = snapshotId();
//
//        System.out.println(snapshotId);


        // 准实时的查询
        DataStream<RowData> stream = FlinkSource
                .forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
//                .startSnapshotId(snapshotId)
                .build();
        stream.print();

        stream.<RowData>assignTimestampsAndWatermarks(WatermarkStrategy
                .<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<RowData>() {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(RowData element, long recordTimestamp) {
                        // 2022-07-15 14:22:42.000
                        return parseTime(element.getString(2).toString(), "yyyy-MM-dd HH:mm:ss.SSS");
                    }
                })).keyBy(new KeySelector<RowData, String>() {
            @Override
            public String getKey(RowData value) throws Exception {
                return value.getString(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<RowData, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<RowData> elements, Collector<String> out) throws Exception {
                        ArrayList<String> rowData = new ArrayList<>();
                        for (RowData rd : elements) {
                            String str = "";
                            str = rd.getString(2).toString()
                                    +" "+rd.getString(2).toString()
                            +" "+parseTimestamp(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                            rowData.add(str);
                        }
                        out.collect(rowData.toString());
                    }
                }).print();



        env.execute("flink-1.14.4");
    }

    public static long snapshotId() {
        //todo:获取上一个快照ID
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String warehousePath = "hdfs://hadoop01:8020/warehouse/iceberg";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of("realtime",
                "rwtest5");
        org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);
        table.refresh();

        Snapshot snapshot = table.currentSnapshot();

        List<HistoryEntry> history = table.history();
        for (HistoryEntry entry : history) {
            long timestampMillis = entry.timestampMillis();
            System.out.println(timestampMillis);
        }

        return snapshot.snapshotId();
    }

    public static String sql() {
        String sql = "CREATE TABLE IF NOT EXISTS rwtest4 (\n" +
                "    `index` STRING,\n" +
                "    `source_type` STRING,\n" +
                "    `source_host` STRING,\n" +
                "    `source_ip` STRING,\n" +
                "    `agent_timestamp` STRING,\n" +
                "    `topic` STRING,\n" +
                "    `file_path` STRING,\n" +
                "    `time` STRING,\n" +
                "    `bs_kcbp_cost_time` FLOAT,\n" +
                "    `bs_kcbp_lbm_num` STRING,\n" +
                "    `batch_no` STRING,\n" +
                "    `initial_time` STRING,\n" +
                "    `kcbp_zzt` STRING,\n" +
                "    `kcbp_bin64` STRING,\n" +
                "    `created_day` STRING\n" +
                ") PARTITIONED BY (`created_day`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='20',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";
        return sql;
    }

    public static String parseTimestamp(long timestamp, String format){
        Date date = new Date(timestamp);
        SimpleDateFormat dateFormat=new SimpleDateFormat(format);//设置当前时间的格式，为年-月-日
        return dateFormat.format(date);
    }

    public  static long parseTime(String time, String format) throws ParseException {
        SimpleDateFormat dateFormat=new SimpleDateFormat(format);
        Date parse = dateFormat.parse(time);
        return parse.getTime();
    }
}
