package top.chendaye666.Strategy.impl;

import top.chendaye666.Strategy.Strategy;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetTimeSpStrategyImpl implements Strategy {
    private String logData = null;

    public GetTimeSpStrategyImpl(String logData) {
        this.logData = logData;
    }

    @Override
    public String get() {
        Matcher mat = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})").matcher(logData);
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
