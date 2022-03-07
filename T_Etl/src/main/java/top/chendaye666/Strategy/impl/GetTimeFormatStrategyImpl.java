package top.chendaye666.Strategy.impl;

import top.chendaye666.Strategy.Strategy;

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GetTimeFormatStrategyImpl implements Strategy {
    private String logData = null;

    public GetTimeFormatStrategyImpl(String logData) {
        this.logData = logData;
    }

    @Override
    public String get() {
        Matcher mat = Pattern.compile("\\[(\\d{8} \\d{9,})").matcher(logData);
        if (mat.find()) {
            String time = mat.group(1);
            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmssSSS");
            Date date = null;
            try {
                date = dateFormat.parse(time, new ParsePosition(0));
                String format = dateFormat.format(date);
                return format;
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }
}
