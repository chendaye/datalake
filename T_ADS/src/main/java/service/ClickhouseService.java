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
     *
     * @param env
     * @param dataStream
     */
    public void insertIntoClickHouse(StreamExecutionEnvironment env, DataStream<L5Entity> dataStream) throws Exception {
        try {
            // sink
            String sql = "INSERT INTO default.l5 (time, contract_no, list_number, fund_number, seat_number) VALUES (?,?,?,?,?)";
            ClickHouseSinkUtil clickHouseSink = new ClickHouseSinkUtil(sql);
            dataStream.addSink(clickHouseSink);
        dataStream.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
        env.execute("clickhouse sink");

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
