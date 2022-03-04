package top.chendaye666.parse.impl;

import top.chendaye666.parse.Parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则匹配解析日志
 */
public class GetRegInxParseImpl implements Parse {
    private String logData = null;
    private String reg = null;
    private int regIndex = 0;

    public GetRegInxParseImpl(String logData, String reg, int regIndex) {
        this.logData = logData;
        this.reg = reg;
        this.regIndex = regIndex;
    }

    @Override
    public String get() {
        return matcherValByReg(logData, reg, regIndex);
    }

    /**
     * 正则解析日志
     * @param log
     * @param reg
     * @param index 第 index 组 模式
     * @return
     */
    public String matcherValByReg(String log, String reg, Integer index) {
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
