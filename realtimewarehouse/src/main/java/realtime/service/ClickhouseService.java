package realtime.service;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import realtime.dao.Ncddzt;
import realtime.dao.NcddztDws;
import realtime.util.ClickHouseSinkUtil;
import realtime.util.ClickHouseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.table.api.Expressions.$;

public class ClickhouseService {
    /**
     * dws 层数据转换成流
     * @param env
     * @param tEnv
     */
    public DataStream<NcddztDws> getStreamFromDws(StreamExecutionEnvironment env, StreamTableEnvironment tEnv){
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/realtime/dws_ncddzt");
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
//                .streaming(true)
                // .startSnapshotId(2120L)
                .build();

        // 从iceberg实时读取数据
        Table table = tEnv.fromDataStream(stream,
                $("source_type"),
                $("agent_timestamp"),
                $("topic"),
                $("total")
        );

        DataStream<NcddztDws> ncddztDataStream = tEnv.toAppendStream(table, NcddztDws.class);

        return ncddztDataStream;
    }

    /**
     * create table dws(source_type String, index String, agent_timestamp String, topic String, total UInt16) ENGINE=TinyLog;
     * @param env
     * @param dataStream
     */
    public void insertIntoClickHouse(StreamExecutionEnvironment env, DataStream<NcddztDws> dataStream) throws Exception {
        // sink
        String sql = "INSERT INTO default.dws (source_type, agent_timestamp, topic, total) VALUES (?,?,?,?)";

        ClickHouseSinkUtil clickHouseSink = new ClickHouseSinkUtil(sql);
        dataStream.addSink(clickHouseSink);
//        dataStream.print();

        env.execute("clickhouse sink");

    }

    /**
     * 测试插入一条数据到 clickhouse
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void insertOneRecord() throws SQLException, ClassNotFoundException {
        // test
        String sql = "INSERT INTO default.dws (source_type, agent_timestamp, topic, total) VALUES (?,?,?,?)";
        Connection connection = ClickHouseUtil.getConnection("hadoop01", 8123, "default");
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, "a");
        preparedStatement.setString(2, "b");
        preparedStatement.setString(3, "v");
        preparedStatement.setLong(4, 777);
        preparedStatement.addBatch();
        int[] len =preparedStatement.executeBatch();
        System.out.println(len.length);
        connection.commit();
    }
}
