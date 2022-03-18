package top.chendaye666.Strategy.impl.auxiliary;

import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.ParamEntity;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * yyyyMMdd HHmmssSSS 转 yyyy-MM-dd HH:mm:ss.SSS
 */
public class DateGtuStrategyImpl implements Strategy<String, String> {

    @Override
    public String get(ParamEntity param, String data) {
        Matcher mat = Pattern.compile("(\\d{8} \\d{9,})").matcher(data);
        if (mat.find()) {
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmssSSS");
            Date date = null;
            try {
                date = dateFormat.parse(data, new ParsePosition(0));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                return simpleDateFormat.format(date.getTime());
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

}
