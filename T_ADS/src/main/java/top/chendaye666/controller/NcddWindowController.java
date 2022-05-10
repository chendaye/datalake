package top.chendaye666.controller;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.service.NcddService;
import top.chendaye666.utils.JsonParamUtils;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Iterator;
import java.util.Random;

public class NcddWindowController {
    public static NcddService ncddService = new NcddService();

    public static void main(String[] args) throws Exception {
        // 参数解析
        ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("path", null);
        JsonParamUtils jsonParam = new JsonParamUtils(path);

        // flink 运行环境
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 开启本地环境 webui：localhouse:8081
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/warehouse/backend");
        env.getCheckpointConfig().setCheckpointTimeout(60000 * 2);
        // tEnv 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/" + jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);

        //todo: 计算L5,委托确认耗时(ms): ncddoiw2:BS_NCDD_OIW_COM[channel] - ncddoiw:BS_NCDD_OIW_WT[channel]， match[val_str]

        //todo：方法一： 可以使用 状态存储 之前的流信息

        //todo: 方法二：拆分流，然后join

        //todo: 方法三：开窗，在窗口内匹配
        stream
                .filter(new FilterFunction<CommonTableEntity>() {
                    @Override
                    public boolean filter(CommonTableEntity value) throws Exception {
                        return value.getSource_type().equals("ncddoiw") || value.getSource_type().equals("ncddoiw2");
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
                            @Override
                            public long extractTimestamp(CommonTableEntity element, long recordTimestamp) {
                                // 模拟乱序数据
//                                Random random = new Random();
//                                long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
//                                return eventTime;
                                // 提取时间戳
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                long eventTime = dateFormat.parse(element.getChannel() + "", new ParsePosition(0)).getTime();
                                return eventTime;
                            }
                        }))
                .setParallelism(1)
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ProcessAllWindowFunction<CommonTableEntity, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CommonTableEntity> elements, Collector<String> out) throws Exception {
                        Iterator<CommonTableEntity> iterator = elements.iterator();
                        CommonTableEntity next = iterator.next();
                        out.collect(next.getVal_str() + " -- " + context.window().getStart() + " -- " + context.window().getEnd());
                    }
                })
                .print("wtf");
        env.execute("stream join");
    }
}
