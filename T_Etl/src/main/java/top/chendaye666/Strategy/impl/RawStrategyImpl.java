package top.chendaye666.Strategy.impl;

import akka.util.Reflect;
import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.pojo.ParamEntity;
import top.chendaye666.reflect.ReflectUtils;
import top.chendaye666.utils.StringUtils;

import java.util.Locale;

/**
 * 直接返回原始数据
 */
public class RawStrategyImpl implements Strategy<LogEntity, String>{

    @Override
    public String get(ParamEntity param, LogEntity data) {
        String field = param.getField().toLowerCase();
        String ret = (String)ReflectUtils.invokeGet(data, "get" + StringUtils.upperCaseFirst(field));
        return ret;
    }
}