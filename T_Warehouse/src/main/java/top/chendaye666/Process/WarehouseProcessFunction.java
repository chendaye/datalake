package top.chendaye666.Process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import top.chendaye666.pojo.CommonTableEntity;

public class WarehouseProcessFunction extends ProcessFunction<CommonTableEntity, String> {
    private static final long serialVersionUID = -8053890528651921943L;

    @Override
    public void processElement(CommonTableEntity value, Context ctx, Collector<String> out) throws Exception {

    }
}
