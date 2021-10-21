package top.chendaye666.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 生产者客户端通过实现接口org.apache.kafka.clients.top.chendaye666.producer.ProducerInterceptor生成一个生产者拦截器。
 *
 * Kafka Producer会在消息序列化和计算分区之前调用拦截器的onSend()方法，用户可以在此方法中进行消息发送前的业务定制。
 * 一般不修改ProducerRecord的topic、key、partition等信息。
 * Kafka Producer会在消息被应答(ack)之前或者消息发送失败时调用拦截器的 onAcknowledgement()方法，
 * 此方法在用户设置的异步CallBack()方法之前执行。onAcknowledgement方法的业务逻辑越简单越好，否则会影响发送性能，
 * 因为该方法运行在Producer的I/O线程中。
 */
public class Interceptor1 implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 在消息体上加时间戳
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp()
        , producerRecord.key(), System.currentTimeMillis()+"_"+producerRecord.value(), producerRecord.headers());

    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
