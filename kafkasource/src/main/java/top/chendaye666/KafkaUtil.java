package top.chendaye666;

import top.chendaye666.producer.AsynchronousProducer;

public class KafkaUtil {
    public static void main(String[] args) throws InterruptedException {
        AsynchronousProducer asynchronousProducer = new AsynchronousProducer();
        asynchronousProducer.producer1();
    }
}
