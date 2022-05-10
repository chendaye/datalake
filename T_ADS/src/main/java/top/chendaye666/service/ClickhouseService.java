package top.chendaye666.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import top.chendaye666.pojo.L5Entity;
import top.chendaye666.pojo.ResultEntity;
import top.chendaye666.process.ClickHouseJoinSink;
import top.chendaye666.process.ClickHouseSink;
import top.chendaye666.utils.ClickHouseUtil;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickhouseService {

    /**
     * create table dws(source_type String, index String, agent_timestamp String, topic String, total UInt16) ENGINE=TinyLog;
     * create table l5(time UInt16, contract_no String, list_number String, fund_number String, seat_number String) ENGINE=TinyLog;
     * create table l5n(time UInt16, contract_no String, list_number String, fund_number String, seat_number String) ENGINE=ReplacingMergeTree() PARTITION BY seat_number ORDER BY  (fund_number, seat_number) PRIMARY KEY (fund_number, seat_number);
     *
     * CREATE TABLE IF NOT EXISTS default.l5r ON CLUSTER '{layer}' (
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
            ClickHouseSink clickHouseSink = new ClickHouseSink(sql);
            dataStream.addSink(clickHouseSink);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * join 结果写入ClickHouse
     * @param dataStream
     */
    public void insertIntoClickHouse2(DataStream<ResultEntity> dataStream){
        // sink
        String sql = "INSERT INTO default.l5join3 (mi, val_str, ncddoiw2_time, ncddoiw_time, diff) VALUES (?,?,?,?,?)";
        ClickHouseJoinSink clickHouseSink = new ClickHouseJoinSink(sql);
        dataStream.addSink(clickHouseSink);

    }

    /**
     * 创建clickhouse 表
     */
    public void createL5Table(){
        // 创建表
        String ddl = "CREATE TABLE IF NOT EXISTS default.l5join3 ON CLUSTER '{layer}' (\n" +
                "     mi String,\n" +
                "     val_str String,\n" +
                "     ncddoiw2_time String,\n" +
                "     ncddoiw_time String,\n" +
                "     diff Int16\n" +
                " ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/default/l5join3', '{replica}')  ORDER BY (val_str );\n";
        ClickHouseUtil.executeDdl(ddl, "hadoop01", 8123);
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
