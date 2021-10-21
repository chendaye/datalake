package top.chendaye666.consumer;

import com.sun.org.apache.xml.internal.utils.XMLStringDefault;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 手动提交 offset
 */
public class Consumer2 {
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
            // 同步提交 线程会阻塞，直到当前批次的 offset 提交成功
            consumer.commitSync();
        }
    }
}

