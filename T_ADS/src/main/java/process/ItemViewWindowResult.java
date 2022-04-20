package process;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ItemViewWindowResult implements WindowFunction<Long, String, Long, TimeWindow> {
    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> input, Collector<String> collector) throws Exception {
        Long itemId = key;
        Long windowEnd = timeWindow.getEnd(); // 用于分组，通过windowEnd 把一个组窗口的所有元素，分组
        Long count  = input.iterator().next(); // 一个窗口的的所有输入聚合成一个，输出只有一个 和，所以迭代器里只有一个值
        collector.collect(windowEnd+"___"+count);
    }
}
