package top.chendaye666.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import top.chendaye666.utils.RandomInt;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 模拟事件时间
 */
public class TimeEventProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String position = "461121";
        int num = 2;
        int index = 2;
        String agent_timestamp = "1627436679538";
        String topic = "";
        String template = "";

        String[] topics = {"topic_1", "topic_2", "topic_3", "topic_4", "topic_5", "topic_6"};

        long lastTime = System.currentTimeMillis();
        //TODO:发送数据
        while (true){
            Thread.sleep(3000);
            index = RandomInt.get(1000,50000);
            agent_timestamp = Long.toString(lastTime+100L);
            lastTime = lastTime+100L;

            position = Integer.toString(RandomInt.get(1000,50000));
            num = RandomInt.get(1,1000);

            topic = topics[RandomInt.get(0,5)];

            template = "{\n" +
                    "\t\"source_type\": \"gtulog\",\n" +
                    "\t\"index\": \""+index+"\",\n" +
                    "\t\"source_host\": \"FKSJYGTU062020\",\n" +
                    "\t\"agent_timestamp\": \""+agent_timestamp+"\",\n" +
                    "\t\"topic\": \""+topic+"\",\n" +
                    "\t\"file_path\": \"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210728\\\\gtulog00005.log\",\n" +
                    "\t\"position\": \""+position+"\",\n" +
                    "\t\"num\": "+num+",\n" +
                    "\t\"log\": \"[20210728 094429966] [     171.18.13.99] [pub1] [00102025] [       0|5884|240828075525001701]  000019930457 MAP01BR0 20210728094429936#|88850776|C1CA1616|00000000000000000000000000000000000000000000001020230635000000001000100001684AQUGyQAAAAAAAAAA0005002301080001T    10000000000AAAAAAAAAAAAMATCH00\\u0000\\u0000\\u0000\\u0000\\u00000899230135\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u00000|88850776|0\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000 3706{\\\"8810\\\":\\\"88850776\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"127.0.0.1\\\",\\\"8813\\\":\\\" \\\",\\\"8814\\\":\\\"123456\\\",\\\"8815\\\":\\\"00102023\\\",\\\"CLI_DEFINE_1\\\":\\\" \\\",\\\"CLI_DEFINE_2\\\":\\\" \\\",\\\"CLI_DEFINE_3\\\":\\\" \\\",\\\"CLI_ORDER_NO\\\":\\\"0\\\",\\\"CUACCT_CODE\\\":\\\"88850776\\\",\\\"CUACCT_TYPE\\\":\\\"0\\\",\\\"CUST_CODE\\\":\\\"20325957\\\",\\\"ERROR_ID\\\":\\\"0\\\",\\\"EXCHANGE_ID\\\":\\\"0\\\",\\\"EXE_BUY_CNT\\\":\\\"1\\\",\\\"EXE_INFO\\\":\\\" \\\",\\\"EXE_SELL_CNT\\\":\\\"0\\\",\\\"FUND_AVL\\\":\\\"610639.7200\\\",\\\"H_FUND_AVL\\\":\\\"0.0000\\\",\\\"INT_ORG\\\":\\\"8071\\\",\\\"IS_WITHDRAW\\\":\\\"F\\\",\\\"MARGIN_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_UFZ\\\":\\\"0.0000\\\",\\\"MARGIN_UFZ\\\":\\\"0.0000\\\",\\\"MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"MATCHED_DATE\\\":\\\"20210728\\\",\\\"MATCHED_PRICE\\\":\\\"137.8330\\\",\\\"MATCHED_QTY\\\":\\\"10\\\",\\\"MATCHED_SN\\\":\\\"0102000011467981\\\",\\\"MATCHED_TIME\\\":\\\"09:44:30\\\",\\\"MATCHED_TYPE\\\":\\\"2\\\",\\\"MATCH_BUY_AMT\\\":\\\"1378.3300\\\",\\\"MATCH_BUY_AVG_PRICE\\\":\\\"137.8330\\\",\\\"MATCH_BUY_QTY\\\":\\\"10\\\",\\\"MATCH_SELL_AMT\\\":\\\"0.0000\\\",\\\"MATCH_SELL_AVG_PRICE\\\":\\\"0.0000\\\",\\\"MATCH_SELL_QTY\\\":\\\"0\\\",\\\"OFFER_RET_MSG\\\":\\\"1委托合法\\\",\\\"ORDER_AMT\\\":\\\"1378.3300\\\",\\\"ORDER_BSN\\\":\\\"12013586\\\",\\\"ORDER_DATE\\\":\\\"20210728\\\",\\\"ORDER_FRZ_AMT\\\":\\\"1378.4300\\\",\\\"ORDER_FUNC_TYPE\\\":\\\"0\\\",\\\"ORDER_ID\\\":\\\"C1CA1616\\\",\\\"ORDER_NO\\\":\\\"3420242409\\\",\\\"ORDER_PRICE\\\":\\\"137.8330\\\",\\\"ORDER_QTY\\\":\\\"10\\\",\\\"ORDER_STATUS\\\":\\\"8\\\",\\\"ORDER_TIME\\\":\\\"2021-07-28 09:44:29.928\\\",\\\"QUERY_POS\\\":\\\"2021072809442993634202424090001\\\",\\\"REMARK1\\\":\\\"3690\\\",\\\"REMARK2\\\":\\\"3700\\\",\\\"REMARK3\\\":\\\"0\\\",\\\"REMARK4\\\":\\\"10\\\",\\\"RLT_SETT_AMT\\\":\\\"1378.4300\\\",\\\"STKBD\\\":\\\"00\\\",\\\"STKEX\\\":\\\"0\\\",\\\"STK_AVL\\\":\\\"10\\\",\\\"STK_BIZ\\\":\\\"100\\\",\\\"STK_BIZ_ACTION\\\":\\\"100\\\",\\\"STK_CODE\\\":\\\"123038\\\",\\\"STK_NAME\\\":\\\"联得转债\\\",\\\"STK_QTY\\\":\\\"10\\\",\\\"STK_TRD_ETFCTN\\\":\\\"0\\\",\\\"STK_TRD_ETFRMN\\\":\\\"0\\\",\\\"STRATEGY_NAME\\\":\\\" \\\",\\\"STRATEGY_TYPE\\\":\\\"0\\\",\\\"SUB_ORDER_SN\\\":\\\"34202424090001\\\",\\\"TOTAL_MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"TOTAL_MATCHED_QTY\\\":\\\"10\\\",\\\"TRDACCT\\\":\\\"0899230135\\\",\\\"TRD_CODE_CLS\\\":\\\"0\\\",\\\"WITHDRAWN_BUY_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_SELL_QTY\\\":\\\"0\\\"}\\r\\r\\n\"\n" +
                    "}";
            System.out.println(template);
            // 有上面异步的例子可以看出，producer的send方法返回对象是Future类型，因此可以通过调用Future对象的get()方法触发同步等待。
            producer.send(new ProducerRecord<String, String>("event_time", template)).get();
        }
//        top.chendaye666.producer.close();
    }


}

