package realtime.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import realtime.dao.NcddztDws;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * 自定义Sink
 */
@Slf4j
public class ClickHouseSinkUtil extends RichSinkFunction<NcddztDws> {
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
     * @param ncddztDws
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(NcddztDws ncddztDws, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, ncddztDws.getSource_type());
        preparedStatement.setString(2, ncddztDws.getAgent_timestamp());
        preparedStatement.setString(3, ncddztDws.getTopic());
        preparedStatement.setLong(4, ncddztDws.getTotal());
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] len = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        log.info("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + len.length);
    }
}
