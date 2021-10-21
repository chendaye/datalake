package top.chendaye666.consumer;

import com.sun.org.apache.xml.internal.utils.XMLStringDefault;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * 自定义 offset
 */
public class Consumer4 {
    public void consumer(){
        HashMap<TopicPartition, Long> currentOffset = new HashMap<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        //  group.id 相同的属于同一个 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, XMLStringDefault.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("iceberg"), new ConsumerRebalanceListener() {
            // 在 Rebalanced 前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                commitOffset(currentOffset);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffset.clear();
                for (TopicPartition partition : collection){
                    // 定位到每个分区最近提交的 offset 位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });

        // 启动消费
        while (true){
            // 一旦拉取到数据就返回，否则最多等待 duration 设置的时间
            ConsumerRecords<String, String> record = consumer.poll(Duration.ofMillis(100));
            record.forEach(r -> {
                System.out.println(r.offset()+"_"+r.key()+"_"+r.value());
            });
            // 提交 offset
            commitOffset(currentOffset);
        }
    }

    /**
     * 获取某分区最新提交的 offset
     * @param partition
     * @return
     */
    private long getOffset(TopicPartition partition){
        return 0;
    }

    /**
     * 提交该消费者所有分区的 offset
     * @param map
     */
    private void commitOffset(Map<TopicPartition, Long> map){}
}

