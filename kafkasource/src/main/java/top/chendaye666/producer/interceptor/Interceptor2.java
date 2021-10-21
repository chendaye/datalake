package top.chendaye666.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 统计 发送成功和失败的数据
 */
public class Interceptor2 implements ProducerInterceptor {

    private AtomicInteger success = new AtomicInteger();
    private AtomicInteger fail = new AtomicInteger();
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return null;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            success.getAndIncrement();
        }else {
            fail.getAndIncrement();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

