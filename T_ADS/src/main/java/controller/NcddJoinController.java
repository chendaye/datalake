package controller;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import pojo.CommonTableEntity;
import pojo.RecordEntity;
import service.NcddService;
import utils.JsonParamUtils;

import java.io.IOException;

/**
 * 使用流join 匹配，求时间差
 */
public class NcddJoinController {
    public static NcddService ncddService = new NcddService();
    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path", null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);

        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/warehouse/backend"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/"+jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);
        stream.filter(new FilterFunction<CommonTableEntity>() {
            @Override
            public boolean filter(CommonTableEntity commonTableEntity) throws Exception {
                return commonTableEntity.getMi().equals("BS_NCDD_SEND_TIME") && commonTableEntity.getChannel3().length() > 0;
            }
        }).print();
        env.execute("stream join");
    }
}
