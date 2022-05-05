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
 * yyyyMMdd HHmmssSSS 解析成时间戳
 */
public class TimestampGtuStrategyImpl implements Strategy<String, String> {
    @Override
    public String get(ParamEntity param, String data) throws ParseException {
        Matcher mat = Pattern.compile("(\\d{8} \\d{9,})").matcher(data);
        if (mat.find()) {
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmssSSS");
            Date date = null;
            date = dateFormat.parse(data);
            return String.valueOf(date.getTime());
        }
        return null;
    }
}
