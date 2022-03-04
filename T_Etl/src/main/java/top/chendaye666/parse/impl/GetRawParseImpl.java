package top.chendaye666.parse.impl;

import top.chendaye666.parse.Parse;

/**
 * 直接返回原始数据
 */
public class GetRawParseImpl implements Parse {
    private String logData = null;

    public GetRawParseImpl(String logData) {
        this.logData = logData;
    }

    @Override
    public String get() {
        return logData;
    }
}
