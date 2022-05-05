package top.chendaye666.Strategy.impl.auxiliary;
import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.ParamEntity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间戳 转  yyyy-MM-dd HH:mm:ss.SSS
 */
public class TimestampSzv5StrategyImpl implements Strategy<String, String> {
    @Override
    public String get(ParamEntity param, String data) throws ParseException {
        String reg = "([0-9]{1,8}-[0-9]{1,6})\\]\\[([0-9]{1,3})";
        Matcher mat = Pattern.compile(reg).matcher(data);
        if (mat.find() && mat.groupCount() == 2) {
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss.SSS");
            String t = mat.group(1)+"."+ mat.group(2);
            // 抛出异常
            Date date = dateFormat.parse(t);
            return String.valueOf(date.getTime());
        }
        return null;
    }
}





