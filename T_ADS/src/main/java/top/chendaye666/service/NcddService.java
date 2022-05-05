package top.chendaye666.service;

import org.apache.flink.types.Row;
import top.chendaye666.dao.NcddDao;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.L5Entity;
import top.chendaye666.process.L5KeyedProcessFunction;

public class NcddService {
    NcddDao dao = new NcddDao();

    public DataStream<CommonTableEntity> getNcddGtuStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath){
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath, true);
        DataStream<CommonTableEntity> stream = tEnv.toDataStream(ncddGtu, CommonTableEntity.class);
        return stream;
    }

    /**
     * 返回 ROW
     * @param env
     * @param tEnv
     * @param tablePath
     * @return
     */
    public DataStream<Row> getRowStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath){
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath, true);
        DataStream<Row> stream = tEnv.toDataStream(ncddGtu);
        return stream;
    }

    /**
     * 返回一个 table
     * @param env
     * @param tEnv
     * @param tablePath
     * @param isStream
     * @return
     */
    public Table getNcddGeneralTable(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath, boolean isStream){
        Table table = dao.getNcddGtu(env, tEnv, tablePath, isStream);
        return table;
    }

    /**
     * 根据流数据计算L5
     * @param stream
     * @return
     */
    public SingleOutputStreamOperator<L5Entity> getL5Stream(DataStream<CommonTableEntity> stream){
        SingleOutputStreamOperator<L5Entity> process = stream
                .filter(new FilterFunction<CommonTableEntity>() {
                    @Override
                    public boolean filter(CommonTableEntity commonTableEntity) throws Exception {
                        return commonTableEntity.getSource_type().equals("ncddoiw2") || commonTableEntity.getSource_type().equals("ncddoiw");
                    }
                })
                .keyBy(new KeySelector<CommonTableEntity, String>() {
                    @Override
                    public String getKey(CommonTableEntity value) throws Exception {
                        // 不能使用 source_type 作为key 分组。state只能用在KeyedStream
                        return value.getVal_str();
                    }
                })
                // 状态仅可在 KeyedStream 上使用，可以通过 stream.keyBy(...) 得到 KeyedStream.
                .process(new L5KeyedProcessFunction());
        return process;
    }
}
