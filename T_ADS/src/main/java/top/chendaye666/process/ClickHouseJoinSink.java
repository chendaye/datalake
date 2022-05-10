package top.chendaye666.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import top.chendaye666.pojo.L5Entity;
import top.chendaye666.pojo.ResultEntity;
import top.chendaye666.utils.ClickHouseUtil;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class ClickHouseJoinSink extends RichSinkFunction<ResultEntity> {
    private static final long serialVersionUID = 6068372633392563852L;
    Connection connection = null;
    String sql;
    String host = "hadoop01";
    String database = "default";
    int port = 8123;

    public ClickHouseJoinSink(String sql){
        this.sql = sql;
    }

    public ClickHouseJoinSink(String host, String database){
        this.host = host;
        this.database = database;
    }

    public ClickHouseJoinSink(String host, String database, String sql){
        this.host = host;
        this.database = database;
        this.sql = sql;
    }

    public ClickHouseJoinSink(String host, String database, int port){
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
     * @param resultEntity
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(ResultEntity resultEntity, Context context) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, resultEntity.getMi());
            preparedStatement.setString(2, resultEntity.getVal_str());
            preparedStatement.setString(3, resultEntity.getNcddoiw2_time());
            preparedStatement.setString(4, resultEntity.getNcddoiw_time());
            preparedStatement.setLong(5, resultEntity.getDiff());
            preparedStatement.addBatch();

            long startTime = System.currentTimeMillis();
            int[] len = preparedStatement.executeBatch();
            connection.commit();
            long endTime = System.currentTimeMillis();
            log.info("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + len.length);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
