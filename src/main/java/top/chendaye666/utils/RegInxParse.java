package top.chendaye666.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则匹配解析日志
 */
public  class  RegInxParse {

    /**
     * 正则解析日志
     * @param log
     * @param reg
     * @param index 第 index 组 模式
     * @return
     */
    public static String matcherValByReg(String log, String reg, Integer index) {
        Matcher mat = Pattern.compile(reg).matcher(log);
        if (mat.find()) {
            if (mat.groupCount() < index) {
                return null;
            }
            return mat.group(index);
        }
        return null;
    }
}
