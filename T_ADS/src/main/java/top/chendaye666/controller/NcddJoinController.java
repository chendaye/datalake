package top.chendaye666.controller;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

        String tablePath = "hdfs://hadoop01:8020/warehouse/iceberg/realtime/" + jsonParam.getJson("baseConf").getString("table");
        DataStream<CommonTableEntity> stream = ncddService.getNcddGtuStream(env, tEnv, tablePath);

        //todo: 计算L5,委托确认耗时(ms): ncddoiw2:BS_NCDD_OIW_COM[channel] - ncddoiw:BS_NCDD_OIW_WT[channel]， match[val_str]

        //todo：方法一： 可以使用 状态存储 之前的流信息

        //todo: 方法二：拆分流，然后join
        OutputTag<CommonTableEntity> ncddoiwTag = new OutputTag<CommonTableEntity>("ncddoiw") {
        };
        OutputTag<CommonTableEntity> ncddoiw2Tag = new OutputTag<CommonTableEntity>("ncddoiw2") {
        };

        SingleOutputStreamOperator<CommonTableEntity> tagStream = stream.process(new ProcessFunction<CommonTableEntity, CommonTableEntity>() {
            @Override
            public void processElement(CommonTableEntity value, Context ctx, Collector<CommonTableEntity> out) throws Exception {
                switch (value.getSource_type()) {
                    case "ncddoiw":
                        ctx.output(ncddoiwTag, value);
                        break;
                    case "ncddoiw2":
                        ctx.output(ncddoiw2Tag, value);
                        break;
                    default:
                        out.collect(value);
                        break;
                }
            }
        });
        // 获取拆分流
        DataStream<CommonTableEntity> ncddoiw2Stream = tagStream.getSideOutput(ncddoiw2Tag);
        DataStream<CommonTableEntity> ncddoiwStream = tagStream.getSideOutput(ncddoiwTag);
        // 使用 event-time 必须要设置 watermark+提取事件时间(ms)
        SingleOutputStreamOperator<CommonTableEntity> ncddoiw2StreamWatermarks = ncddoiw2Stream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>
                        forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
                            @Override
                            public long extractTimestamp(CommonTableEntity commonTableEntity, long l) {
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                long timestamp = dateFormat.parse(commonTableEntity.getChannel(), new ParsePosition(0)).getTime();
                                return timestamp;
                            }
                        }));

        SingleOutputStreamOperator<CommonTableEntity> ncddoiwStreamWatermarks = ncddoiwStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>
                        forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
                            @Override
                            public long extractTimestamp(CommonTableEntity commonTableEntity, long l) {
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                long timestamp = dateFormat.parse(commonTableEntity.getChannel(), new ParsePosition(0)).getTime();
                                long s = System.currentTimeMillis() + 2;
                                return s;
                            }
                        }));

        ncddoiwStreamWatermarks
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(50)))
//                .top.chendaye666.process(new ProcessAllWindowFunction<CommonTableEntity, String, TimeWindow>() {
//                    @Override
//                    public void top.chendaye666.process(Context context, Iterable<CommonTableEntity> elements, Collector<String> out) throws Exception {
//                        CommonTableEntity next = elements.iterator().next();
//                        out.collect(context.window().getStart()+" "+context.window().getEnd());
//                    }
//                })
                .sum("val")
                .print("wtf");
        // join 流
//        DataStream<Long> joinStream = ncddoiw2StreamWatermarks.join(ncddoiwStreamWatermarks).where(new KeySelector<CommonTableEntity, String>() {
//            @Override
//            public String getKey(CommonTableEntity commonTableEntity) throws Exception {
//                // 第一个输入的元素 key
//                return commonTableEntity.getVal_str();
//            }
//        }).equalTo(new KeySelector<CommonTableEntity, String>() {
//            @Override
//            public String getKey(CommonTableEntity commonTableEntity) throws Exception {
//                // 第二个输入的元素 key
//                return commonTableEntity.getVal_str();
//            }
//        }).window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
//                .apply(new JoinFunction<CommonTableEntity, CommonTableEntity, Long>() {
//                    @Override
//                    public Long join(CommonTableEntity commonTableEntity, CommonTableEntity commonTableEntity2) throws Exception {
//                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                        long timestamp = dateFormat.parse(commonTableEntity.getChannel(), new ParsePosition(0)).getTime();
//                        long timestamp2 = dateFormat.parse(commonTableEntity2.getChannel(), new ParsePosition(0)).getTime();
//                        System.out.println(timestamp);
//                        return timestamp - timestamp2;
//                    }
//                });
//        joinStream.print("ff");
        env.execute("stream join");
    }
}
