package top.chendaye666.Strategy.impl.auxiliary;

import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.pojo.ParamEntity;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeSpStrategyImpl implements Strategy<LogEntity, String> {

    @Override
    public String get(ParamEntity param, LogEntity data) {
        Matcher mat = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})").matcher(data.getLog());
        if (mat.find()) {
            String time = mat.group(0);
            String replace = time.replace(',', '.');
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date date = new Date();
            try {
                date = dateFormat.parse(replace);
                return String.valueOf(date.getTime());
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
