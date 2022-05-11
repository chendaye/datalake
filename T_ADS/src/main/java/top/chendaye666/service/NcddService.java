package top.chendaye666.service;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
import top.chendaye666.pojo.ResultEntity;
import top.chendaye666.process.L5KeyedProcessFunction;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class NcddService {
    NcddDao dao = new NcddDao();

    public DataStream<CommonTableEntity> getNcddGtuStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath) {
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath, true);
        DataStream<CommonTableEntity> stream = tEnv.toDataStream(ncddGtu, CommonTableEntity.class);
        return stream;
    }

    /**
     * 返回 ROW
     *
     * @param env
     * @param tEnv
     * @param tablePath
     * @return
     */
    public DataStream<Row> getRowStream(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath) {
        Table ncddGtu = dao.getNcddGtu(env, tEnv, tablePath, true);
        DataStream<Row> stream = tEnv.toDataStream(ncddGtu);
        return stream;
    }

    /**
     * 返回一个 table
     *
     * @param env
     * @param tEnv
     * @param tablePath
     * @param isStream
     * @return
     */
    public Table getNcddGeneralTable(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tablePath, boolean isStream) {
        Table table = dao.getNcddGtu(env, tEnv, tablePath, isStream);
        return table;
    }

    /**
     * 根据流数据计算L5
     *
     * @param stream
     * @return
     */
    public SingleOutputStreamOperator<L5Entity> getL5Stream(DataStream<CommonTableEntity> stream) {
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

    /**
     * 先拆分流，然后再join流，获取匹配流之间的时间差
     *
     * @param stream
     * @return
     */
    public DataStream<ResultEntity> joinL5Stream(DataStream<CommonTableEntity> stream) {
        //todo: 计算L5,委托确认耗时(ms): ncddoiw2:BS_NCDD_OIW_COM[channel] - ncddoiw:BS_NCDD_OIW_WT[channel]， match[val_str]

        //todo：方法一： 可以使用 状态存储 之前的流信息

        //todo: 方法二：拆分流，然后join
        OutputTag<CommonTableEntity> ncddoiwTag = new OutputTag<CommonTableEntity>("ncddoiw") {
        };
        OutputTag<CommonTableEntity> ncddoiw2Tag = new OutputTag<CommonTableEntity>("ncddoiw2") {
        };

        SingleOutputStreamOperator<CommonTableEntity> tagStream = stream
                .filter(new FilterFunction<CommonTableEntity>() {
                    @Override
                    public boolean filter(CommonTableEntity value) throws Exception {
                        return value.getSource_type().equals("ncddoiw") || value.getSource_type().equals("ncddoiw2");
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CommonTableEntity>
                        forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<CommonTableEntity>() {
                            @Override
                            public long extractTimestamp(CommonTableEntity commonTableEntity, long l) {
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                long timestamp = dateFormat.parse(commonTableEntity.getChannel(), new ParsePosition(0)).getTime();
                                return timestamp;
                            }
                        }))
                .setParallelism(1)
                .process(new ProcessFunction<CommonTableEntity, CommonTableEntity>() {
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
        // join 流
        DataStream<ResultEntity> joinStream = ncddoiw2Stream.join(ncddoiwStream).where(new KeySelector<CommonTableEntity, String>() {
            @Override
            public String getKey(CommonTableEntity commonTableEntity) throws Exception {
                // 第一个输入的元素 key
                return commonTableEntity.getVal_str();
            }
        }).equalTo(new KeySelector<CommonTableEntity, String>() {
            @Override
            public String getKey(CommonTableEntity commonTableEntity) throws Exception {
                // 第二个输入的元素 key
                return commonTableEntity.getVal_str();
            }
        })
                // 滑动，两个窗口之间重叠的部分恰好等于两个匹配记录之间的时间差,防止两个匹配的记录不在一个window里面
                // 窗口之间重叠部分越少，越接近于滚动窗口，重复匹配的概率就低
                .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(800)))
                .apply(new JoinFunction<CommonTableEntity, CommonTableEntity, ResultEntity>() {
                    @Override
                    public ResultEntity join(CommonTableEntity commonTableEntity2, CommonTableEntity commonTableEntity) throws Exception {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        long timestamp2 = dateFormat.parse(commonTableEntity2.getChannel(), new ParsePosition(0)).getTime();
                        long timestamp = dateFormat.parse(commonTableEntity.getChannel(), new ParsePosition(0)).getTime();

                        return new ResultEntity(commonTableEntity2.getMi(), commonTableEntity2.getVal_str(), commonTableEntity2.getChannel(), commonTableEntity.getChannel(), timestamp2 - timestamp);
                    }
                });
        return joinStream;
    }
}
