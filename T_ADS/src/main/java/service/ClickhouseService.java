package service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import pojo.L5Entity;
import utils.ClickHouseSinkUtil;
import utils.ClickHouseUtil;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.table.api.Expressions.$;

public class ClickhouseService {

    /**
     * create table dws(source_type String, index String, agent_timestamp String, topic String, total UInt16) ENGINE=TinyLog;
     * create table l5(time UInt16, contract_no String, list_number String, fund_number String, seat_number String) ENGINE=TinyLog;
     * create table l5n(time UInt16, contract_no String, list_number String, fund_number String, seat_number String) ENGINE=ReplacingMergeTree() PARTITION BY seat_number ORDER BY  (fund_number, seat_number) PRIMARY KEY (fund_number, seat_number);
     *
     * CREATE TABLE IF NOT EXISTS default.l5r ON CLUSTER '{cluster}' (
     *     time        UInt16,
     *     contract_no String,
     *     list_number String,
     *     fund_number String,
     *     seat_number String
     * ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default/l5r', '{replica}') PARTITION BY contract_no ORDER BY (fund_number) PRIMARY KEY (fund_number) SETTINGS index_granularity = 8192;
     *
     * CREATE TABLE IF NOT EXISTS default.l5r_all ON CLUSTER '{layer}' AS default.l5r ENGINE = Distributed('{layer}',default,l5r,rand());
     * @param dataStream
     */
    public void insertIntoClickHouse(DataStream<L5Entity> dataStream) throws Exception {
        try {
            // sink
//            dataStream.print("wtf");
            String sql = "INSERT INTO default.l5m (time, contract_no, list_number, fund_number, seat_number) VALUES (?,?,?,?,?)";
            ClickHouseSinkUtil clickHouseSink = new ClickHouseSinkUtil(sql);
            dataStream.addSink(clickHouseSink);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试插入一条数据到 clickhouse
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void insertOneRecord() throws SQLException, ClassNotFoundException {
        // test
        String sql = "INSERT INTO default.l5 (time, contract_no, list_number, fund_number, seat_number) VALUES (?,?,?,?,?)";
        Connection connection = ClickHouseUtil.getConnection("hadoop01", 8123, "default");
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, 777);
        preparedStatement.setString(2, "b");
        preparedStatement.setString(3, "v");
        preparedStatement.setString(4, "a");
        preparedStatement.setString(5, "a7");
        preparedStatement.addBatch();
        int[] len = preparedStatement.executeBatch();
        System.out.println(len.length);
        connection.commit();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        ClickhouseService clickhouseService = new ClickhouseService();

        clickhouseService.insertOneRecord();
    }
}
