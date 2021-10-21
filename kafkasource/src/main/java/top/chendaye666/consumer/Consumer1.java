package top.chendaye666.consumer;

import com.sun.org.apache.xml.internal.utils.XMLStringDefault;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 自动提交 offset
 *
 * 自动位移提交(commit)的动作是在poll()方法里完成的，每次向服务端发起拉取请求之前会检查是否可以进行位移提交，
 * 如果可以，那么就会提交上一次轮询的位移。自动提交消费位移的方式非常简便，免去了复杂的位移提交逻辑，使得应用层代码非常简洁。
 * 如果在下一次自动提交消费位移之前，消费者宕机了，那么又得从上一次位移提交的地方重新开始消费，这将导致重复消费。
 * 可以减小位移提交的时间间隔来减小消息重复的时间窗口，但是这会使移提交更加频繁。
 */
public class Consumer1 {
    public void consumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //  group.id 相同的属于同一个 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交，1s一次
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());

        // 使用拦截器
//        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Interceptor1.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("iceberg"));

        // 启动消费
        while (true){
            // 一旦拉取到数据就返回，否则最多等待 duration 设置的时间
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            record.forEach(r -> {
                System.out.println(r.offset()+"_"+r.key()+"_"+r.value());
            });

        }
    }
}

