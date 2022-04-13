package service;

import dao.NcddDao;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pojo.CommonTableEntity;

public class NcddService {
    NcddDao dao = new NcddDao();

    public DataStream<CommonTableEntity> getNcddGtuStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath){
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath);
        DataStream<CommonTableEntity> stream = tEnv.toAppendStream(ncddGtu, CommonTableEntity.class);
        return stream;
    }
}
