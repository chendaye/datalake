package top.chendaye666.consumer;

import com.sun.org.apache.xml.internal.utils.XMLStringDefault;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 异步提交 offset
 */
public class Consumer3 {
    public void consumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //  group.id 相同的属于同一个 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("iceberg"));

        // 启动消费
        while (true){
            // 一旦拉取到数据就返回，否则最多等待 duration 设置的时间
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            record.forEach(r -> {
                System.out.println(r.offset()+"_"+r.key()+"_"+r.value());
            });
            // 异步提交，可以带回调参数，线程不会阻塞
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e != null){
                        System.out.println("提交失败"+map);
                    }
                }
            });
        }
    }
}

