package process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.CommonTableEntity;
import pojo.L5Entity;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

public class L5KeyedProcessFunction extends KeyedProcessFunction<String, CommonTableEntity, L5Entity> {
    // MspState
    private MapState<String, CommonTableEntity> commonTableEntityMapState = null;
    private MapState<String, CommonTableEntity> commonTableEntityMapState2 = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        commonTableEntityMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, CommonTableEntity>("CommonTableEntityMapState", String.class, CommonTableEntity.class));
        commonTableEntityMapState2 = getRuntimeContext().getMapState(new MapStateDescriptor<String, CommonTableEntity>("CommonTableEntityMapState2", String.class, CommonTableEntity.class));
    }

    @Override
    public void processElement(CommonTableEntity value, Context ctx, Collector<L5Entity> out) throws Exception {
        String key = value.getVal_str();

        switch (value.getSource_type()) {
            case "ncddoiw":
//                                out.collect("ncddoiw "+commonTableEntityMapState2.contains(key));
                if (commonTableEntityMapState2.contains(key)) {
                    long diff = getTime(commonTableEntityMapState2.get(key).getChannel())
                            - getTime(value.getChannel());

                    // 清除状态
                    commonTableEntityMapState2.remove(key);
                    if (commonTableEntityMapState.contains(key)) {
                        commonTableEntityMapState.remove(key);
                    }
                    if (diff > 0)
                        out.collect(new L5Entity(diff, value.getChannel(), value.getChannel2(), value.getVal_str(), value.getNode()));
                } else {
                    commonTableEntityMapState.put(key, value);
                }
                break;
            case "ncddoiw2":
//                                out.collect("ncddoiw2 "+commonTableEntityMapState.contains(key));
                if (commonTableEntityMapState.contains(key)) {
                    long diff = getTime(value.getChannel())
                            - getTime(commonTableEntityMapState.get(key).getChannel());
                    commonTableEntityMapState.remove(key);
                    if (commonTableEntityMapState2.contains(key)) {
                        commonTableEntityMapState2.remove(key);
                    }
                    if (diff > 0)
                        out.collect(new L5Entity(diff, value.getChannel(), value.getChannel2(), value.getVal_str(), value.getNode()));
                } else {
                    commonTableEntityMapState2.put(key, value);
                }
                break;
            default:
                return;
        }
    }

    public long getTime(String str) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timestamp = dateFormat.parse(str, new ParsePosition(0)).getTime();
        return timestamp;
    }
}
