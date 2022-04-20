package service;

import dao.NcddDao;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import pojo.CommonTableEntity;
import pojo.L5Entity;
import process.L5KeyedProcessFunction;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

public class NcddService {
    NcddDao dao = new NcddDao();

    public DataStream<CommonTableEntity> getNcddGtuStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath){
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath, true);
        DataStream<CommonTableEntity> stream = tEnv.toAppendStream(ncddGtu, CommonTableEntity.class);
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
                        return "key";
                    }
                })
                // 状态仅可在 KeyedStream 上使用，可以通过 stream.keyBy(...) 得到 KeyedStream.
                .process(new L5KeyedProcessFunction());
        return process;
    }
}
