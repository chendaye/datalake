package top.chendaye666.Strategy;

import top.chendaye666.pojo.ParamEntity;
import top.chendaye666.reflect.ReflectUtils;

import java.lang.reflect.Constructor;

/**
 * https://refactoringguru.cn/design-patterns/strategy
 */
public class ParseContext<I, O> {
    private Strategy<I, O> strategy;

    public ParseContext(){}

    public Strategy<I, O> getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy<I, O> strategy) {
        this.strategy = strategy;
    }

    /**
     * 执行策略
     * @param param
     * @param data
     * @return
     */
    public O executeStrategy(ParamEntity param, I data){
        try {
            String stgy = param.getStrategy();
            O firstVal = strategy.get(param, data);
            if (stgy != null){
                Strategy<O,O> obj = (Strategy<O,O>)ReflectUtils.getObj(stgy);
                return obj.get(param,firstVal);
            }
            return firstVal;
        }catch (Exception e){
            e.printStackTrace();
            // 解析失败返回 null
            return null;
        }
    }


}
