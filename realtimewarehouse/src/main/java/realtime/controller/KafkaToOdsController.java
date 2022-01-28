package realtime.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realtime.service.CommonService;
import realtime.service.OdsService;

@Slf4j
public class KafkaToOdsController {
    public static CommonService commonService = new CommonService();
    public static OdsService odsService = new OdsService();

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // hadoop catalog
        commonService.createHadoopCatalog(tEnv);
        // 创建 kafka session表
        odsService.createKafkaTable(tEnv);
        // 插入 ods 表
        odsService.insertToOds(tEnv);
    }
}
