# 安装 spark3.2

## 配置 
[Hadoop3.2 +Spark3.0全分布式安装](https://zhuanlan.zhihu.com/p/97693616)

- scala 替换成1.12
- 解压
- 参考 `cdh spark`配置 `vim  /etc/spark/conf/spark-env.sh`，改写spark3 的`spark-env.sh`
- 编辑 `workers` 文件,设置spark集群节点
- 复制目录到其他节点
- `sbin` 目录启动集群 `start-all.sh  stop-all.sh`
- `webui http://hadoop01:8080/`


[Apache Spark整合Hive](https://www.jianshu.com/p/75b440c9b904)

- 安装配置HIVE（cdh省了）
- ` cd /etc/hive/conf/hdfs-site.xml,core-site.xml,mared-reduce.xml,hive-site.xml,yarn-site.xml` 复制一份到 `spark/conf`
- 同步文件

# 依赖的jar包

## flink+spark+iceberg 版本选择
- flink：1.11.2
- scala: 2.12
- hadoop: 3.0.0-cdh6.3.2
- spark： 3.2.0


### Iceberg 版本 0.12.0

#### Flink

> 以下jar 放入 flink/lib

[flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0_2.11/1.11.2/flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar)

[flink-sql-connector-kafka_2.11-1.11.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.2/flink-sql-connector-kafka_2.11-1.11.2.jar)

[iceberg-flink-runtime-0.12.0.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.12.0/iceberg-flink-runtime-0.12.0.jar)

### Hive

> 以下jar 放入 cd /opt/cloudera/parcels/CDH/lib/hive/lib

[iceberg-hive-runtime-0.12.0.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.12.0/iceberg-hive-runtime-0.12.0.jar)

#### Spark3.2

> 以下jar 放入 cd spark/jars

[iceberg-spark3-runtime-0.12.0.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.12.0/iceberg-spark3-runtime-0.12.0.jar)

[iceberg-hive-runtime-0.12.0.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.12.0/iceberg-hive-runtime-0.12.0.jar)



#### Spark2（CDH）

> 以下jar 放入 cd /opt/cloudera/parcels/CDH/lib/spark/jars

[iceberg-spark-runtime-0.12.0.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.12.0/iceberg-spark-runtime-0.12.0.jar)


# Spark 整合 Iceberg

# 创建 Catalog

> 注意：基于Hive的Catalog只能加载Iceberg表，如果想加载非Iceberg表需要使用SparkSessionCatalog

> 从Iceberg0.11.0及其更高版本，向Spark3.0增加了新的SQL命令支持，例如CALL 存储过程或者ALTER TABLE ... WRITE ORDERED BY等。
如果想使用这些SQL命令需要将 IcebergSparkSessionExtensions 的Spark属性添加到Spark环境中


```bash
spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse
```

## Hive Catalog

```bash
spark-sql 
  --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0 \ 
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \ 
  --conf spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog \ 
  --conf spark.sql.catalog.hive_prod.type=hive \ 
  --conf spark.sql.catalog.hive_prod.uri=thrift://hadoop01:9083
  
  
# 或者进入 spark-sql shell
set spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
set spark.sql.catalog.hive_prod.type = hive
set spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
# omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml

```

## Hadoop Catalog

```bash
# 创建
spark-sql 
  --master local[*] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_prod=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_prod.type=hadoop \
  --conf spark.sql.catalog.hadoop_prod.warehouse=hdfs://hadoop01:8020/warehouse/iceberg
  
  
  
# 或者
set spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
set spark.sql.catalog.hadoop_prod.type = hadoop
set spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path

# To see the current catalog and namespace, run 
show current namespace;

```


## 创建表

### Shell 创建

```sql
create database  hadoop_prod.t1;

use hadoop_prod.t1;

CREATE TABLE hadoop_prod.t1.test (id bigint, data string) USING iceberg

INSERT INTO hadoop_prod.t1.test VALUES (1, 'a'), (2, 'b'), (3, 'c');

```
### API 创建


# 其他

[SparkSQL DatasourceV2 之 Multiple Catalog](https://developer.aliyun.com/article/756968)

[Iceberg 开发指南](https://intl.cloud.tencent.com/zh/document/product/1026/41079)

[7000字图文掌握SparkSQL操作Iceberg](https://kknews.cc/code/vn8o5va.html)

[SPARK-SQL - catalog()操作数据库，表等相关的元信息](https://blog.csdn.net/qq_41712271/article/details/107933243)

[数据湖之iceberg系列(四)iceberg-spark编程](https://blog.csdn.net/qq_37933018/article/details/110483423)

[iceberg-Spark3.0SQL 测试案例](https://blog.csdn.net/weixin_45681127/article/details/115459565)

[V2](https://tianchi.aliyun.com/forum/postDetail?postId=111897)

[Spark Configuration](https://iceberg.apache.org/spark-configuration/#using-catalogs)