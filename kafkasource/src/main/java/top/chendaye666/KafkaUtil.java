package top.chendaye666;

import top.chendaye666.producer.AsynchronousProducer;

/**
 * ps -ef | grep java
 * ps -ef | grep kafkasource*
 * java -jar kafkasource-1.0-SNAPSHOT.jar
 *  nohup java -jar kafkasource-1.0-SNAPSHOT.jar 2>&1 > /dev/null &
 *  kafka-console-consumer --bootstrap-server hadoop01:9092 --from-beginning --topic ods_ncddzt
 */
public class KafkaUtil {
    public static void main(String[] args) throws InterruptedException {
        AsynchronousProducer asynchronousProducer = new AsynchronousProducer();
        asynchronousProducer.producer3();
    }
}
