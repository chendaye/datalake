package top.chendaye666.Strategy;

import top.chendaye666.pojo.ParamEntity;

/**
 * 解析日志数据
 */
public interface Strategy<I, O> {
    /*解析日志数据*/
    O get(ParamEntity param, I data);
}
