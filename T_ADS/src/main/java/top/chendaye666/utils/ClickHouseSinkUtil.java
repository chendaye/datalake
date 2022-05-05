package top.chendaye666.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import top.chendaye666.pojo.L5Entity;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * 自定义Sink
 */
@Slf4j
public class ClickHouseSinkUtil extends RichSinkFunction<L5Entity> {
    Connection connection = null;
    String sql;
    String host = "hadoop01";
    String database = "default";
    int port = 8123;

    public ClickHouseSinkUtil(String sql){
        this.sql = sql;
    }

    public ClickHouseSinkUtil(String host, String database){
        this.host = host;
        this.database = database;
    }

    public ClickHouseSinkUtil(String host, String database, String sql){
        this.host = host;
        this.database = database;
        this.sql = sql;
    }

    public ClickHouseSinkUtil(String host, String database, int port){
        this.host = host;
        this.database = database;
        this.port = port;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        connection = ClickHouseUtil.getConnection(host, port, database);
    }

    @Override
    public void close()throws Exception {
        super.close();
        if (connection != null) connection.close();
    }

    /**
     * sink 逻辑
     * @param l5Entity
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(L5Entity l5Entity, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, l5Entity.getTime());
        preparedStatement.setString(2, l5Entity.getContractNo());
        preparedStatement.setString(3, l5Entity.getListNumber());
        preparedStatement.setString(4, l5Entity.getFundNumber());
        preparedStatement.setString(5, l5Entity.getSeatNumber());
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] len = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        log.info("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + len.length);
    }
}
