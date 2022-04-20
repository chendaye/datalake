package process;

import org.apache.flink.api.common.functions.AggregateFunction;
import pojo.CommonTableEntity;

public class CountAgg implements AggregateFunction<CommonTableEntity, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(CommonTableEntity userBehavior, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc, Long acc1) {
        return acc + acc1;
    }
}