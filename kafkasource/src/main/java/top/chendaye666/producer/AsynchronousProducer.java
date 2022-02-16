package top.chendaye666.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import top.chendaye666.producer.interceptor.Interceptor1;
import top.chendaye666.producer.interceptor.Interceptor2;
import top.chendaye666.utils.DateUtil;
import top.chendaye666.utils.RandomInt;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 异步发送消息：不需要等ack
 *
 * KafkaProducer：生产者对象，用来发送数据
 * ProducerConfig：设置生产者的一系列配置参数
 * ProducerRecord：每条数据都要封装成一个ProducerRecord 对象
 * Producer客户端代码
 */
public class AsynchronousProducer {
    public  void producer1() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 生产拦截器
        List<String> interceptor = new ArrayList<>();
        interceptor.add(Interceptor1.class.getName());
        interceptor.add(Interceptor2.class.getName());
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] data = new String[3];
        data[0] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"191820\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627350258536\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210727\\\\gtulog00003.log\",\"POSITION\":\"90667539\",\"LOG\":\"[20210727 094417868] [    171.18.13.100] [send] [10388101] [bb4f9716ce0945fb8fd97caaeda7f0e9]  000005840457 MAP01BA0 20210727094417907bb4f9716ce0945fb8fd97caaeda7f0e900000729D8BF231700000729D8BF2B54103881010101030100001000100000357BQfJAAAAAAAAAAAA00230026001010000000000AAAAAAAAAAAA301,socket,both,4004,8556>40396122653706{\\\"t0\\\":[{\\\"8817\\\":\\\"0\\\",\\\"8818\\\":\\\"0\\\",\\\"8819\\\":\\\"业务请求已接受\\\"}],\\\"t1\\\":[{\\\"38\\\":\\\"700\\\",\\\"40\\\":\\\"100\\\",\\\"48\\\":\\\"600888\\\",\\\"55\\\":\\\"新疆众和\\\",\\\"66\\\":\\\"106202897\\\",\\\"8834\\\":\\\"20210727\\\",\\\"8842\\\":\\\"700\\\",\\\"8844\\\":\\\"20210727\\\",\\\"8845\\\":\\\"2021-07-27 09:44:17.907\\\",\\\"8859\\\":\\\"20210727\\\",\\\"8920\\\":\\\"79197221\\\",\\\"9101\\\":\\\"0\\\",\\\"9102\\\":\\\"235143178\\\",\\\"9103\\\":\\\"1\\\",\\\"9106\\\":\\\"1066741397\\\",\\\"9107\\\":\\\"10667413970001\\\",\\\"916\\\":\\\"0\\\",\\\"917\\\":\\\"0\\\"}]}\\r\\r\\n\"}";
        data[1] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"191818\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627350258536\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210727\\\\gtulog00003.log\",\"POSITION\":\"90666000\",\"LOG\":\"[20210727 094417866] [    171.18.13.100] [recv] [10388101] [bb4f9716ce0945fb8fd97caaeda7f0e9]  000007320457 MAP01BR0 00000000000000000bb4f9716ce0945fb8fd97caaeda7f0e900000000000000000000000000000000103881010000000000001000100000576AAAAAAAAAAAAAAAA3706{\\\"38\\\":\\\"700.000\\\",\\\"40\\\":\\\"100\\\",\\\"44\\\":\\\"8.310\\\",\\\"448\\\":\\\"E062304923\\\",\\\"48\\\":\\\"600888\\\",\\\"625\\\":\\\"10\\\",\\\"8810\\\":\\\"34324468\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"PC;IIP=39.108.143.229;IPORT=14871;LIP=172.18.17.80;MAC=00163E141CCE;HD=WZ9I1RF2UMUEM5HVBSCL;PCN=IZSG02JNYNJWO9Z;CPU=0F8BFBFF00050654;PI=C^NTFS^39G;VOL=AAB7-EF14;@XTQMT;1.0.0.21949\\\",\\\"8813\\\":\\\"P\\\",\\\"8814\\\":\\\"139134324468        2107272107272359590000000001TbCo1359MbQ=eFJtxYSMx1QvCtpjLYHviUf+6DLmXBSFBwMmsZ9Xatg=\\\",\\\"8815\\\":\\\"10388101\\\",\\\"8816\\\":\\\"20210727094417000\\\",\\\"8821\\\":\\\"8161\\\",\\\"8826\\\":\\\"3\\\",\\\"8842\\\":\\\"700\\\",\\\"8902\\\":\\\"34324468\\\",\\\"8920\\\":\\\"79197221\\\",\\\"9101\\\":\\\"0\\\",\\\"9102\\\":\\\"235143178\\\"}\\r\\r\\n\"}";
        data[2] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"1394629\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627436679538\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210728\\\\gtulog00005.log\",\"POSITION\":\"22203179\",\"LOG\":\"[20210728 094429966] [     171.18.13.99] [pub1] [00102025] [       0|5884|240828075525001701]  000019930457 MAP01BR0 20210728094429936#|88850776|C1CA1616|00000000000000000000000000000000000000000000001020230635000000001000100001684AQUGyQAAAAAAAAAA0005002301080001T    10000000000AAAAAAAAAAAAMATCH00\\u0000\\u0000\\u0000\\u0000\\u00000899230135\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u00000|88850776|0\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000 3706{\\\"8810\\\":\\\"88850776\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"127.0.0.1\\\",\\\"8813\\\":\\\" \\\",\\\"8814\\\":\\\"123456\\\",\\\"8815\\\":\\\"00102023\\\",\\\"CLI_DEFINE_1\\\":\\\" \\\",\\\"CLI_DEFINE_2\\\":\\\" \\\",\\\"CLI_DEFINE_3\\\":\\\" \\\",\\\"CLI_ORDER_NO\\\":\\\"0\\\",\\\"CUACCT_CODE\\\":\\\"88850776\\\",\\\"CUACCT_TYPE\\\":\\\"0\\\",\\\"CUST_CODE\\\":\\\"20325957\\\",\\\"ERROR_ID\\\":\\\"0\\\",\\\"EXCHANGE_ID\\\":\\\"0\\\",\\\"EXE_BUY_CNT\\\":\\\"1\\\",\\\"EXE_INFO\\\":\\\" \\\",\\\"EXE_SELL_CNT\\\":\\\"0\\\",\\\"FUND_AVL\\\":\\\"610639.7200\\\",\\\"H_FUND_AVL\\\":\\\"0.0000\\\",\\\"INT_ORG\\\":\\\"8071\\\",\\\"IS_WITHDRAW\\\":\\\"F\\\",\\\"MARGIN_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_UFZ\\\":\\\"0.0000\\\",\\\"MARGIN_UFZ\\\":\\\"0.0000\\\",\\\"MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"MATCHED_DATE\\\":\\\"20210728\\\",\\\"MATCHED_PRICE\\\":\\\"137.8330\\\",\\\"MATCHED_QTY\\\":\\\"10\\\",\\\"MATCHED_SN\\\":\\\"0102000011467981\\\",\\\"MATCHED_TIME\\\":\\\"09:44:30\\\",\\\"MATCHED_TYPE\\\":\\\"2\\\",\\\"MATCH_BUY_AMT\\\":\\\"1378.3300\\\",\\\"MATCH_BUY_AVG_PRICE\\\":\\\"137.8330\\\",\\\"MATCH_BUY_QTY\\\":\\\"10\\\",\\\"MATCH_SELL_AMT\\\":\\\"0.0000\\\",\\\"MATCH_SELL_AVG_PRICE\\\":\\\"0.0000\\\",\\\"MATCH_SELL_QTY\\\":\\\"0\\\",\\\"OFFER_RET_MSG\\\":\\\"1委托合法\\\",\\\"ORDER_AMT\\\":\\\"1378.3300\\\",\\\"ORDER_BSN\\\":\\\"12013586\\\",\\\"ORDER_DATE\\\":\\\"20210728\\\",\\\"ORDER_FRZ_AMT\\\":\\\"1378.4300\\\",\\\"ORDER_FUNC_TYPE\\\":\\\"0\\\",\\\"ORDER_ID\\\":\\\"C1CA1616\\\",\\\"ORDER_NO\\\":\\\"3420242409\\\",\\\"ORDER_PRICE\\\":\\\"137.8330\\\",\\\"ORDER_QTY\\\":\\\"10\\\",\\\"ORDER_STATUS\\\":\\\"8\\\",\\\"ORDER_TIME\\\":\\\"2021-07-28 09:44:29.928\\\",\\\"QUERY_POS\\\":\\\"2021072809442993634202424090001\\\",\\\"REMARK1\\\":\\\"3690\\\",\\\"REMARK2\\\":\\\"3700\\\",\\\"REMARK3\\\":\\\"0\\\",\\\"REMARK4\\\":\\\"10\\\",\\\"RLT_SETT_AMT\\\":\\\"1378.4300\\\",\\\"STKBD\\\":\\\"00\\\",\\\"STKEX\\\":\\\"0\\\",\\\"STK_AVL\\\":\\\"10\\\",\\\"STK_BIZ\\\":\\\"100\\\",\\\"STK_BIZ_ACTION\\\":\\\"100\\\",\\\"STK_CODE\\\":\\\"123038\\\",\\\"STK_NAME\\\":\\\"联得转债\\\",\\\"STK_QTY\\\":\\\"10\\\",\\\"STK_TRD_ETFCTN\\\":\\\"0\\\",\\\"STK_TRD_ETFRMN\\\":\\\"0\\\",\\\"STRATEGY_NAME\\\":\\\" \\\",\\\"STRATEGY_TYPE\\\":\\\"0\\\",\\\"SUB_ORDER_SN\\\":\\\"34202424090001\\\",\\\"TOTAL_MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"TOTAL_MATCHED_QTY\\\":\\\"10\\\",\\\"TRDACCT\\\":\\\"0899230135\\\",\\\"TRD_CODE_CLS\\\":\\\"0\\\",\\\"WITHDRAWN_BUY_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_SELL_QTY\\\":\\\"0\\\"}\\r\\r\\n\"}";
        String value = null;

        String position = "461121";
        int num = 2;
        long index = 2;
        String agent_timestamp = "1627436679538";
        String topic = "";
        String template = "";
        String source_type = "";

        String[] topics = {"topic_1", "topic_2", "topic_3", "topic_4", "topic_5", "topic_6"};
        String[] types = {"send", "recv", "pub1"};

        long lastTime = System.currentTimeMillis();
        //TODO:发送数据
        long ind = 0L;
        while (true){
            Thread.sleep(3000); // 1s 一条
            index = ++ind;
            agent_timestamp = Long.toString(lastTime+100L);
            lastTime = lastTime+100L;
            position = Integer.toString(RandomInt.get(1000,50000));
            num = RandomInt.get(1,5);
            topic = topics[RandomInt.get(0,5)];

            source_type = DateUtil.ToDate();

            template = "{\n" +
                    "\t\"SOURCE_TYPE\": \""+source_type+"\",\n" +
                    "\t\"INDEX\": \""+index+"\",\n" +
                    "\t\"SOURCE_HOST\": \"FKSJYGTU062020\",\n" +
                    "\t\"AGENT_TIMESTAMP\": \""+agent_timestamp+"\",\n" +
                    "\t\"TOPIC\": \""+topic+"\",\n" +
                    "\t\"FILE_PATH\": \"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210728\\\\gtulog00005.log\",\n" +
                    "\t\"POSITION\": \""+position+"\",\n" +
                    "\t\"NUM\": "+num+",\n" +
                    "\t\"LOG\": \"[20210728 094429966] [     171.18.13.99] ["+types[RandomInt.get(0,3)]+"] [00102025] [       0|5884|240828075525001701]  000019930457 MAP01BR0 20210728094429936#|88850776|C1CA1616|00000000000000000000000000000000000000000000001020230635000000001000100001684AQUGyQAAAAAAAAAA0005002301080001T    10000000000AAAAAAAAAAAAMATCH00\\u0000\\u0000\\u0000\\u0000\\u00000899230135\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u00000|88850776|0\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000 3706{\\\"8810\\\":\\\"88850776\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"127.0.0.1\\\",\\\"8813\\\":\\\" \\\",\\\"8814\\\":\\\"123456\\\",\\\"8815\\\":\\\"00102023\\\",\\\"CLI_DEFINE_1\\\":\\\" \\\",\\\"CLI_DEFINE_2\\\":\\\" \\\",\\\"CLI_DEFINE_3\\\":\\\" \\\",\\\"CLI_ORDER_NO\\\":\\\"0\\\",\\\"CUACCT_CODE\\\":\\\"88850776\\\",\\\"CUACCT_TYPE\\\":\\\"0\\\",\\\"CUST_CODE\\\":\\\"20325957\\\",\\\"ERROR_ID\\\":\\\"0\\\",\\\"EXCHANGE_ID\\\":\\\"0\\\",\\\"EXE_BUY_CNT\\\":\\\"1\\\",\\\"EXE_INFO\\\":\\\" \\\",\\\"EXE_SELL_CNT\\\":\\\"0\\\",\\\"FUND_AVL\\\":\\\"610639.7200\\\",\\\"H_FUND_AVL\\\":\\\"0.0000\\\",\\\"INT_ORG\\\":\\\"8071\\\",\\\"IS_WITHDRAW\\\":\\\"F\\\",\\\"MARGIN_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_UFZ\\\":\\\"0.0000\\\",\\\"MARGIN_UFZ\\\":\\\"0.0000\\\",\\\"MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"MATCHED_DATE\\\":\\\"20210728\\\",\\\"MATCHED_PRICE\\\":\\\"137.8330\\\",\\\"MATCHED_QTY\\\":\\\"10\\\",\\\"MATCHED_SN\\\":\\\"0102000011467981\\\",\\\"MATCHED_TIME\\\":\\\"09:44:30\\\",\\\"MATCHED_TYPE\\\":\\\"2\\\",\\\"MATCH_BUY_AMT\\\":\\\"1378.3300\\\",\\\"MATCH_BUY_AVG_PRICE\\\":\\\"137.8330\\\",\\\"MATCH_BUY_QTY\\\":\\\"10\\\",\\\"MATCH_SELL_AMT\\\":\\\"0.0000\\\",\\\"MATCH_SELL_AVG_PRICE\\\":\\\"0.0000\\\",\\\"MATCH_SELL_QTY\\\":\\\"0\\\",\\\"OFFER_RET_MSG\\\":\\\"1委托合法\\\",\\\"ORDER_AMT\\\":\\\"1378.3300\\\",\\\"ORDER_BSN\\\":\\\"12013586\\\",\\\"ORDER_DATE\\\":\\\"20210728\\\",\\\"ORDER_FRZ_AMT\\\":\\\"1378.4300\\\",\\\"ORDER_FUNC_TYPE\\\":\\\"0\\\",\\\"ORDER_ID\\\":\\\"C1CA1616\\\",\\\"ORDER_NO\\\":\\\"3420242409\\\",\\\"ORDER_PRICE\\\":\\\"137.8330\\\",\\\"ORDER_QTY\\\":\\\"10\\\",\\\"ORDER_STATUS\\\":\\\"8\\\",\\\"ORDER_TIME\\\":\\\"2021-07-28 09:44:29.928\\\",\\\"QUERY_POS\\\":\\\"2021072809442993634202424090001\\\",\\\"REMARK1\\\":\\\"3690\\\",\\\"REMARK2\\\":\\\"3700\\\",\\\"REMARK3\\\":\\\"0\\\",\\\"REMARK4\\\":\\\"10\\\",\\\"RLT_SETT_AMT\\\":\\\"1378.4300\\\",\\\"STKBD\\\":\\\"00\\\",\\\"STKEX\\\":\\\"0\\\",\\\"STK_AVL\\\":\\\"10\\\",\\\"STK_BIZ\\\":\\\"100\\\",\\\"STK_BIZ_ACTION\\\":\\\"100\\\",\\\"STK_CODE\\\":\\\"123038\\\",\\\"STK_NAME\\\":\\\"联得转债\\\",\\\"STK_QTY\\\":\\\"10\\\",\\\"STK_TRD_ETFCTN\\\":\\\"0\\\",\\\"STK_TRD_ETFRMN\\\":\\\"0\\\",\\\"STRATEGY_NAME\\\":\\\" \\\",\\\"STRATEGY_TYPE\\\":\\\"0\\\",\\\"SUB_ORDER_SN\\\":\\\"34202424090001\\\",\\\"TOTAL_MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"TOTAL_MATCHED_QTY\\\":\\\"10\\\",\\\"TRDACCT\\\":\\\"0899230135\\\",\\\"TRD_CODE_CLS\\\":\\\"0\\\",\\\"WITHDRAWN_BUY_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_SELL_QTY\\\":\\\"0\\\"}\\r\\r\\n\"\n" +
                    "}";
            System.out.println(template);
            producer.send(new ProducerRecord<String, String>("ods_ncddzt", template));
//            top.chendaye666.producer.send(new ProducerRecord<String, String>("iceberg", template));
        }
//        top.chendaye666.producer.close();
    }

    /**
     * 回调函数会在producer收到ack时异步调用，该方法有两个参数：RecordMetadata、Exception。
     * 这两个参数是互斥的关系，即如果Exception为null，则消息发送成功，此时RecordMetadata必定不为null。
     * 消息发送异常时，RecordMetadata为null，而exception不为null。消息发送失败会自动重试，不需在回调函数中手动重试。
     * 重试次数由参数retries设定。
     */
    public  void producer2(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 32);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 生产拦截器
        List<String> interceptor = new ArrayList<>();
        interceptor.add(Interceptor1.class.getName());
        interceptor.add(Interceptor2.class.getName());
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] data = new String[3];
        data[0] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"191820\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627350258536\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210727\\\\gtulog00003.log\",\"POSITION\":\"90667539\",\"LOG\":\"[20210727 094417868] [    171.18.13.100] [send] [10388101] [bb4f9716ce0945fb8fd97caaeda7f0e9]  000005840457 MAP01BA0 20210727094417907bb4f9716ce0945fb8fd97caaeda7f0e900000729D8BF231700000729D8BF2B54103881010101030100001000100000357BQfJAAAAAAAAAAAA00230026001010000000000AAAAAAAAAAAA301,socket,both,4004,8556>40396122653706{\\\"t0\\\":[{\\\"8817\\\":\\\"0\\\",\\\"8818\\\":\\\"0\\\",\\\"8819\\\":\\\"业务请求已接受\\\"}],\\\"t1\\\":[{\\\"38\\\":\\\"700\\\",\\\"40\\\":\\\"100\\\",\\\"48\\\":\\\"600888\\\",\\\"55\\\":\\\"新疆众和\\\",\\\"66\\\":\\\"106202897\\\",\\\"8834\\\":\\\"20210727\\\",\\\"8842\\\":\\\"700\\\",\\\"8844\\\":\\\"20210727\\\",\\\"8845\\\":\\\"2021-07-27 09:44:17.907\\\",\\\"8859\\\":\\\"20210727\\\",\\\"8920\\\":\\\"79197221\\\",\\\"9101\\\":\\\"0\\\",\\\"9102\\\":\\\"235143178\\\",\\\"9103\\\":\\\"1\\\",\\\"9106\\\":\\\"1066741397\\\",\\\"9107\\\":\\\"10667413970001\\\",\\\"916\\\":\\\"0\\\",\\\"917\\\":\\\"0\\\"}]}\\r\\r\\n\"}";
        data[1] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"191818\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627350258536\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210727\\\\gtulog00003.log\",\"POSITION\":\"90666000\",\"LOG\":\"[20210727 094417866] [    171.18.13.100] [recv] [10388101] [bb4f9716ce0945fb8fd97caaeda7f0e9]  000007320457 MAP01BR0 00000000000000000bb4f9716ce0945fb8fd97caaeda7f0e900000000000000000000000000000000103881010000000000001000100000576AAAAAAAAAAAAAAAA3706{\\\"38\\\":\\\"700.000\\\",\\\"40\\\":\\\"100\\\",\\\"44\\\":\\\"8.310\\\",\\\"448\\\":\\\"E062304923\\\",\\\"48\\\":\\\"600888\\\",\\\"625\\\":\\\"10\\\",\\\"8810\\\":\\\"34324468\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"PC;IIP=39.108.143.229;IPORT=14871;LIP=172.18.17.80;MAC=00163E141CCE;HD=WZ9I1RF2UMUEM5HVBSCL;PCN=IZSG02JNYNJWO9Z;CPU=0F8BFBFF00050654;PI=C^NTFS^39G;VOL=AAB7-EF14;@XTQMT;1.0.0.21949\\\",\\\"8813\\\":\\\"P\\\",\\\"8814\\\":\\\"139134324468        2107272107272359590000000001TbCo1359MbQ=eFJtxYSMx1QvCtpjLYHviUf+6DLmXBSFBwMmsZ9Xatg=\\\",\\\"8815\\\":\\\"10388101\\\",\\\"8816\\\":\\\"20210727094417000\\\",\\\"8821\\\":\\\"8161\\\",\\\"8826\\\":\\\"3\\\",\\\"8842\\\":\\\"700\\\",\\\"8902\\\":\\\"34324468\\\",\\\"8920\\\":\\\"79197221\\\",\\\"9101\\\":\\\"0\\\",\\\"9102\\\":\\\"235143178\\\"}\\r\\r\\n\"}";
        data[2] = "{\"SOURCE_TYPE\":\"gtulog\",\"INDEX\":\"1394629\",\"SOURCE_HOST\":\"FKSJYGTU062020\",\"AGENT_TIMESTAMP\":\"1627436679538\",\"TOPIC\":\"ncddzt\",\"FILE_PATH\":\"D:\\\\ztzt\\\\maServer_jygtu\\\\0_bin\\\\x64\\\\run\\\\log\\\\20210728\\\\gtulog00005.log\",\"POSITION\":\"22203179\",\"LOG\":\"[20210728 094429966] [     171.18.13.99] [pub1] [00102025] [       0|5884|240828075525001701]  000019930457 MAP01BR0 20210728094429936#|88850776|C1CA1616|00000000000000000000000000000000000000000000001020230635000000001000100001684AQUGyQAAAAAAAAAA0005002301080001T    10000000000AAAAAAAAAAAAMATCH00\\u0000\\u0000\\u0000\\u0000\\u00000899230135\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u00000|88850776|0\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000 3706{\\\"8810\\\":\\\"88850776\\\",\\\"8811\\\":\\\"1\\\",\\\"8812\\\":\\\"127.0.0.1\\\",\\\"8813\\\":\\\" \\\",\\\"8814\\\":\\\"123456\\\",\\\"8815\\\":\\\"00102023\\\",\\\"CLI_DEFINE_1\\\":\\\" \\\",\\\"CLI_DEFINE_2\\\":\\\" \\\",\\\"CLI_DEFINE_3\\\":\\\" \\\",\\\"CLI_ORDER_NO\\\":\\\"0\\\",\\\"CUACCT_CODE\\\":\\\"88850776\\\",\\\"CUACCT_TYPE\\\":\\\"0\\\",\\\"CUST_CODE\\\":\\\"20325957\\\",\\\"ERROR_ID\\\":\\\"0\\\",\\\"EXCHANGE_ID\\\":\\\"0\\\",\\\"EXE_BUY_CNT\\\":\\\"1\\\",\\\"EXE_INFO\\\":\\\" \\\",\\\"EXE_SELL_CNT\\\":\\\"0\\\",\\\"FUND_AVL\\\":\\\"610639.7200\\\",\\\"H_FUND_AVL\\\":\\\"0.0000\\\",\\\"INT_ORG\\\":\\\"8071\\\",\\\"IS_WITHDRAW\\\":\\\"F\\\",\\\"MARGIN_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_FRZ\\\":\\\"0.0000\\\",\\\"MARGIN_PRE_UFZ\\\":\\\"0.0000\\\",\\\"MARGIN_UFZ\\\":\\\"0.0000\\\",\\\"MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"MATCHED_DATE\\\":\\\"20210728\\\",\\\"MATCHED_PRICE\\\":\\\"137.8330\\\",\\\"MATCHED_QTY\\\":\\\"10\\\",\\\"MATCHED_SN\\\":\\\"0102000011467981\\\",\\\"MATCHED_TIME\\\":\\\"09:44:30\\\",\\\"MATCHED_TYPE\\\":\\\"2\\\",\\\"MATCH_BUY_AMT\\\":\\\"1378.3300\\\",\\\"MATCH_BUY_AVG_PRICE\\\":\\\"137.8330\\\",\\\"MATCH_BUY_QTY\\\":\\\"10\\\",\\\"MATCH_SELL_AMT\\\":\\\"0.0000\\\",\\\"MATCH_SELL_AVG_PRICE\\\":\\\"0.0000\\\",\\\"MATCH_SELL_QTY\\\":\\\"0\\\",\\\"OFFER_RET_MSG\\\":\\\"1委托合法\\\",\\\"ORDER_AMT\\\":\\\"1378.3300\\\",\\\"ORDER_BSN\\\":\\\"12013586\\\",\\\"ORDER_DATE\\\":\\\"20210728\\\",\\\"ORDER_FRZ_AMT\\\":\\\"1378.4300\\\",\\\"ORDER_FUNC_TYPE\\\":\\\"0\\\",\\\"ORDER_ID\\\":\\\"C1CA1616\\\",\\\"ORDER_NO\\\":\\\"3420242409\\\",\\\"ORDER_PRICE\\\":\\\"137.8330\\\",\\\"ORDER_QTY\\\":\\\"10\\\",\\\"ORDER_STATUS\\\":\\\"8\\\",\\\"ORDER_TIME\\\":\\\"2021-07-28 09:44:29.928\\\",\\\"QUERY_POS\\\":\\\"2021072809442993634202424090001\\\",\\\"REMARK1\\\":\\\"3690\\\",\\\"REMARK2\\\":\\\"3700\\\",\\\"REMARK3\\\":\\\"0\\\",\\\"REMARK4\\\":\\\"10\\\",\\\"RLT_SETT_AMT\\\":\\\"1378.4300\\\",\\\"STKBD\\\":\\\"00\\\",\\\"STKEX\\\":\\\"0\\\",\\\"STK_AVL\\\":\\\"10\\\",\\\"STK_BIZ\\\":\\\"100\\\",\\\"STK_BIZ_ACTION\\\":\\\"100\\\",\\\"STK_CODE\\\":\\\"123038\\\",\\\"STK_NAME\\\":\\\"联得转债\\\",\\\"STK_QTY\\\":\\\"10\\\",\\\"STK_TRD_ETFCTN\\\":\\\"0\\\",\\\"STK_TRD_ETFRMN\\\":\\\"0\\\",\\\"STRATEGY_NAME\\\":\\\" \\\",\\\"STRATEGY_TYPE\\\":\\\"0\\\",\\\"SUB_ORDER_SN\\\":\\\"34202424090001\\\",\\\"TOTAL_MATCHED_AMT\\\":\\\"1378.3300\\\",\\\"TOTAL_MATCHED_QTY\\\":\\\"10\\\",\\\"TRDACCT\\\":\\\"0899230135\\\",\\\"TRD_CODE_CLS\\\":\\\"0\\\",\\\"WITHDRAWN_BUY_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_QTY\\\":\\\"0\\\",\\\"WITHDRAWN_SELL_QTY\\\":\\\"0\\\"}\\r\\r\\n\"}";
        String value = null;
        //TODO:发送数据
        while (true){
            value = data[RandomInt.get(0,2)];
            producer.send(new ProducerRecord<String, String>("iceberg", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println("Success:"+recordMetadata.offset());
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }
//        top.chendaye666.producer.close();
    }
}

