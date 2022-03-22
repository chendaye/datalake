package top.chendaye666.Process;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.chendaye666.pojo.CommonTableEntity;

public class WarehouseKeyedProcessFunction extends KeyedProcessFunction<String, CommonTableEntity, String> {
    private static final long serialVersionUID = 4176802539985963879L;

    @Override
    public void processElement(CommonTableEntity value, Context ctx, Collector<String> out) throws Exception {
        ctx.getCurrentKey();
    }
}
