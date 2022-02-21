package realtime.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import realtime.dao.Mock;

import static org.apache.flink.table.api.Expressions.$;

public class MockService {
    /**
     * https://blog.csdn.net/zhangdongan1991/article/details/105796613
     * @param tEnv
     */
    public void createKafkaTable(StreamTableEnvironment tEnv){
        // 建库
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tEnv.useDatabase("kafka");
        tEnv.executeSql("DROP TABLE IF EXISTS mock_kafka");
        // 建表
        String kafkaOdsNcddzt = "CREATE TABLE IF NOT EXISTS mock_kafka (\n" +
                "    `id` BIGINT,\n" +
                "    `topic` STRING,\n" +
                "    `name` STRING,\n" +
                "    `num` FLOAT,\n" +
                "    `ts` BIGINT,\n" +
                "    t AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss'))\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',  -- using kafka connector\n" +
                "    'topic' = 'mock',  -- kafka topic\n" +
                "    'properties.group.id' = 'mock',  -- reading from the beginning\n" +
                "    'scan.startup.mode' = 'latest-offset',  -- latest-offset\n" +
                "    'properties.bootstrap.servers' = 'hadoop01:9092',  -- kafka broker address\n" +
                "    'format' = 'json'  -- the data format is json\n" +
                ")";
        tEnv.executeSql(kafkaOdsNcddzt);
    }

    /**
     * TIMESTAMP(3) 是 秒级，不是 毫秒 级
     * @param tEnv
     */
    public void createMockTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.mock (\n" +
                "    `id` BIGINT,\n" +
                "    `topic` STRING,\n" +
                "    `name` STRING,\n" +
                "    `num` FLOAT,\n" +
                "    `ts` TIMESTAMP(3)\n" +
                ") PARTITIONED BY (`topic`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }

    public void insertToMock(StreamTableEnvironment tEnv){
        String sinkSql = "INSERT INTO  hadoop_prod.realtime.mock" +
                " SELECT " +
                "`id` ," +
                " `topic`, " +
                "`name`," +
                "`num`," +
                " t as ts" +
                " FROM " +
                "kafka.mock_kafka";
        tEnv.executeSql(sinkSql);
    }

    public void readMock(StreamExecutionEnvironment env, StreamTableEnvironment tEnv){
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/realtime/mock");
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
//                 .startSnapshotId(5755344342814085506L)
                .build();
//         stream.print();

        // 从iceberg实时读取数据
        Table table = tEnv.fromDataStream(stream,
                $("id"),
                $("topic"),
                $("name"),
                $("num")
        );
        // table 转为 AppendStream 进行处理
        DataStream<Mock> mockStream = tEnv.toAppendStream(table, Mock.class);
        mockStream.print();
    }
}
