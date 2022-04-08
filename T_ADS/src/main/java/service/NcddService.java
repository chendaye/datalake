package service;

import dao.NcddDao;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pojo.RecordEntity;

public class NcddService {
    NcddDao dao = new NcddDao();

    public DataStream<RecordEntity> getNcddGtuStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath){
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath);
        DataStream<RecordEntity> stream = tEnv.toAppendStream(ncddGtu, RecordEntity.class);
        stream.print();
        return null;
    }
}
