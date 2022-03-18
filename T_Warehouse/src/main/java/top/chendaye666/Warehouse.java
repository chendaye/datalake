package top.chendaye666;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import top.chendaye666.Process.WarehouseFlatMap;
import top.chendaye666.Service.WarehouseTableService;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.NcddLogEntity;
import top.chendaye666.utils.JsonParamUtils;

import static org.apache.flink.table.api.Expressions.$;

public class Warehouse {
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

        // catalog
        WarehouseTableService warehouseTableService = new WarehouseTableService();
        warehouseTableService.createHadoopCatalog(tEnv);

        // 读ncdd_log 表
        // table 转 流
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_log");
        // 准实时的查询
        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                // .startSnapshotId(2120L)
                .build();
//         stream.print();
        Table table = tEnv.fromDataStream(stream,
                $("date"),
                $("log")
        );

        // table 转为 AppendStream 进行处理
        tEnv.toAppendStream(table, NcddLogEntity.class)
                .flatMap(new WarehouseFlatMap(jsonParam.getJson("sourceType")))
        .print();


        env.execute("Warehouse");
    }
}
