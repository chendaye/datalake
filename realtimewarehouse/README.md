# 逐条生产数据测试

```bash
# 创建 topic
kafka-topics.sh --zookeeper hadoop01:2181/kafka --create --replication-factor 3 --partitions 1 --topic mock
# 删除 topic
kafka-topics.sh --zookeeper hadoop01:2181/kafka --delete --topic mock
# 生产数据
kafka-console-producer.sh --broker-list hadoop01:9092 --topic mock
# 消费数据
kafka-console-consumer.sh --bootstrap-server hadoop01:9092 --from-beginning --topic mock
```

> 数据格式

`13位时间戳：精度 ms`
`10位时间戳：精度 s`

```json
{"id":23,"topic":"A","name":"lisa","num":76.8,"ts":1642435233328}
```