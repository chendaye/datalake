package top.chendaye666;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.junit.Test;
import top.chendaye666.pojo.Ncddzt;

public class ReadIcebergTest {
    @Test
    public void flinkReadSparkCreateIcebergTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));

        EnvironmentSettings blinkStreamSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
        System.setProperty("HADOOP_USER_NAME", "root");

        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/t1/test");
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
        DataStream<Row> dataStream = tEnv.toAppendStream(table, Row.class);
        dataStream.print();
        env.execute("");
    }
}
