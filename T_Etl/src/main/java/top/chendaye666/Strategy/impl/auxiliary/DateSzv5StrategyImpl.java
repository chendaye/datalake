package top.chendaye666.Strategy.impl.auxiliary;

import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.ParamEntity;
import java.text.SimpleDateFormat;

/**
 * "time2":"20220304-100740][942"
 * ([0-9]{1,8}-[0-9]{1,6}[0-9]\]\[[0-9]{1,3})
 *
 * 20220304-100740][942 转
 */
public class DateSzv5StrategyImpl implements Strategy<String, String> {
    @Override
    public String get(ParamEntity param, String data) {
        // 10位时间戳精确到 m， 13位时间戳精确到 ms
        if (data.length() == 13 || data.length() == 10){
            long timestamp = Long.parseLong(data);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            return format.format(timestamp);
        }
        return null;
    }
}

