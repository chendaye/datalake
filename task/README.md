# 小文件问题
[深度集成 Flink: Apache Iceberg 0.11.0 最新功能解读](https://zhuanlan.zhihu.com/p/349420627)

> 目前在 Apache Iceberg 中，将提供 3 中方式来解决小文件问题

## 方法一

>  在 Iceberg 表中设置 write.distribution-mode=hash 属性

```sql
CREATE TABLE sample (
    id BIGINT,
    data STRING
) PARTITIONED BY (data) WITH (
    'write.distribution-mode'='hash'
);
```

```
这样可以保证每一条记录按照 partition key 做 shuffle 之后再写入，每一个 Partition 最多由一个 Task 来负责写入，大大地减少了小文件的产生。但是，这很容易产生另外一个问题，就是数据倾斜的问题。很多业务表都是按照时间字段来做分区的，而产生的新数据都是按照时间写入的，容易导致新数据都写入同一个 partition，造成写入数据热点。目前我们推荐的做法是，在 partition 下面采用 hash 的方式设置 bucket，那么每一个 partition 的数据将均匀地落到每个 bucket 内，每一个 bucket 最多只会由一个 task 来写，既解决了小文件问题，又解决了数据热点问题。

在 Flink 1.11 版本暂时不支持通过 SQL 的方式创建 bucket，但我们可以通过 Java API 的方式将上述按照 data 字段 partition 之后的表添加 bucket。调用方式如下：
```

```java
table.updateSpec()
       .addField(Expressions.bucket("id", 32))
       .commit();
```

[汽车之家：基于 Flink + Iceberg 的湖仓一体架构实践](https://zhuanlan.zhihu.com/p/379561951)

## 方法二

> 定期对 Apache Iceberg 表执行 Major Compaction 来合并 Apache iceberg 表中的小文件。这个作业目前是一个 Flink 的批作业，提供 Java API 的方式来提交作业

[参考](https://github.com/apache/iceberg/blob/master/site/docs/flink.md#rewrite-files-action)


## 方法三

> 在每个 Flink Sink 流作业之后，外挂算子用来实现小文件的自动合并。
> 这个功能目前暂未 merge 到社区版本，由于涉及到 format v2 的 compaction 的一些讨论，我们会在 0.12.0 版本中发布该功能。


# 运维

[Recommended Maintenance](https://iceberg.apache.org/maintenance/)

[Configuration](https://iceberg.apache.org/configuration/#configuration)

[Iceberg 合并小文件并删除历史](https://blog.csdn.net/M283592338/article/details/120769331)

[Iceberg 实践 | 基于 Flink CDC 打通数据实时入湖](https://jishuin.proginn.com/p/763bfbd5bdbe)

[基于Flink CDC打通数据实时入湖](https://www.163.com/dy/article/GC8GAT0A0511FQO9.html)

## 压缩小文件

## 快照过期处理

## 清理orphan文件

## 删除元数据文件



