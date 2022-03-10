package top.chendaye666.Strategy.impl;

import com.alibaba.fastjson.JSONObject;
import top.chendaye666.Strategy.Strategy;
import top.chendaye666.pojo.LogEntity;
import top.chendaye666.pojo.NcddLogEntity;
import top.chendaye666.pojo.ParamEntity;
import top.chendaye666.reflect.ReflectUtils;
import top.chendaye666.utils.StringUtils;

/**
 * 取log.log
 */
public class RawLogStrategyImpl implements Strategy<LogEntity, String> {

    @Override
    public String get(ParamEntity param, LogEntity data) {
        // 取log值，转NcddLogEntity
        String log = data.getLog();
        NcddLogEntity ncddLogEntity = JSONObject.parseObject(log, NcddLogEntity.class);
        // 取 NcddLogEntity 值
        String field = param.getField().toLowerCase();
        String ret = (String) ReflectUtils.invokeGet(ncddLogEntity, StringUtils.upperCaseFirst(field));
        return ret;
    }
}
