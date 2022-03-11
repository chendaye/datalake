package top.chendaye666;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.Service.EtlLog;
import top.chendaye666.Service.EtlToTable;
import top.chendaye666.utils.JsonParamUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;


/**
 * ETL 数据
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/connectors/kafka.html
 */
public class Etl {
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

        // etl log
        EtlLog etlLog = new EtlLog();
        SingleOutputStreamOperator<String> etl = etlLog.etl(jsonParam, env);

        // insert table
        EtlToTable etlToTable = new EtlToTable();
        etlToTable.createHadoopCatalog(tEnv);
        etlToTable.createOdsTable(tEnv);
        etlToTable.insert(tEnv, etl);

        env.execute("ETL");
    }


}
