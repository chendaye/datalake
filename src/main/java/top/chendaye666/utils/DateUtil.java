package top.chendaye666.utils;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	public static final String sdf = "yyyyMMdd HHmmssSSS";
	public static final String sdf2 = "yyyyMMddHHmmssSSS";
	public static final String sdf4 = "yyyyMMdd-HHmmssSSS";
	public static final String sdf1 = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final  SimpleDateFormat sd = new SimpleDateFormat("yyyyMMddHHmmss");

	public static Date LongtoDate(String time){
		return new Date(Long.valueOf(time));
	}

	public static String ToDate(String time,String pattern){
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		return df.format(new Date(Long.valueOf(time)));
	}

	public static String ToDate(long time,String pattern){
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		return df.format(new Date(time));
	}

	public static String ToDate(){
		Date date = new Date();
		SimpleDateFormat df = new SimpleDateFormat(sdf1);
		return df.format(new Date(date.getTime()));
	}

	public static long formatToTimestamp(String timeString, String format){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
		Date time = simpleDateFormat.parse(timeString, new ParsePosition(0));
		return time.getTime();
	}

}
