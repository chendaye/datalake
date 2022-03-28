package top.chendaye666.Process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.chendaye666.pojo.CommonTableEntity;
import top.chendaye666.pojo.RecordEntity;

import java.util.HashSet;
import java.util.Iterator;

/**
 * 入库
 */
public class WarehouseProcessFunction extends ProcessFunction<CommonTableEntity, RecordEntity> {
    private static final long serialVersionUID = -8053890528651921943L;

    /**
     * 给每一条记录打标签 tag
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(CommonTableEntity value, Context ctx, Collector<RecordEntity> out) throws Exception {
        RecordEntity record = new RecordEntity(
                value.getSource_type(),
                value.getMi(),
                value.getTime(),
                value.getDate(),
                value.getCreated_at(),
                value.getNode(),
                value.getChannel(),
                value.getChannel2(),
                value.getChannel3(),
                value.getChannel4(),
                value.getChannel5(),
                value.getChannel6(),
                value.getVal(),
                value.getVal_str());
        ctx.output( new OutputTag<RecordEntity>(value.getTable_name()){}, record);
//        out.collect(record);
    }
}
