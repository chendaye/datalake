package top.chendaye666.Strategy;

import top.chendaye666.pojo.ParamEntity;

/**
 * https://refactoringguru.cn/design-patterns/strategy
 */
public class ParseContext<I, O> {
    private Strategy<I, O> strategy;

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
        return strategy.get(param, data);
    }
}
