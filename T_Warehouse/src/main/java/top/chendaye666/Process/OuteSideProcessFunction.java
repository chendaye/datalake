package top.chendaye666.Process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.chendaye666.pojo.CommonTableEntity;

import java.util.HashMap;
import java.util.HashSet;

/**
 * 拆分流
 */
public class OuteSideProcessFunction extends ProcessFunction<CommonTableEntity, CommonTableEntity> {

    OutputTag<CommonTableEntity> generalTag = new OutputTag<CommonTableEntity>("ncdd_general"){};
    OutputTag<CommonTableEntity> gtulogTag = new OutputTag<CommonTableEntity>("ncdd_gtulog"){};

    @Override
    public void open(Configuration parameters) throws Exception {
    }


    @Override
    public void processElement(CommonTableEntity commonTableEntity, Context context, Collector<CommonTableEntity> collector) throws Exception {
        String tag = commonTableEntity.getTable_name();
        // 给每条 数据打标签
        OutputTag<CommonTableEntity> outputTag = new OutputTag<CommonTableEntity>(tag) {
        };
        context.output(outputTag, commonTableEntity);

    }
}
