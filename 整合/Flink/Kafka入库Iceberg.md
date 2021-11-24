# 前期准备

## flink+iceberg 版本选择

- flink：1.11.2
- scala: 2.11
- iceberg: 0.11.1
- hive: 2.1.1
- kafka: 2.4.1
- hadoop:  3.0.0-cdh6.3.2


## 依赖的jar包

### Iceberg 版本 0.11.2

> 以下jar 放入 flink/lib

> scala 2.12 版本也在下面的地址

[flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0_2.11/1.11.2/flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar)

[flink-sql-connector-kafka_2.11-1.11.2.jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.11.2/flink-sql-connector-kafka_2.11-1.11.2.jar)

[iceberg-flink-runtime-0.11.1.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.11.1/iceberg-flink-runtime-0.11.1.jar)

> 以下jar 放入 hive/lib

[iceberg-hive-runtime-0.11.1.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.11.1/iceberg-hive-runtime-0.11.1.jar)

# Kafka

## 基本操作

```bash
# 查看当前服务器中的所有topic
kafka-topics --zookeeper hadoop01:2181/kafka --list

# 创建topic
kafka-topics --zookeeper hadoop01:2181/kafka --create --replication-factor 3 --partitions 1 --topic first

# 删除topic，需要server.properties中设置delete.topic.enable=true否则只是标记删除。
kafka-topics --zookeeper hadoop01:2181/kafka --delete --topic first


# 发送消息
kafka-console-producer --broker-list hadoop01:9092 --topic first

# 消费消息
kafka-console-consumer --bootstrap-server hadoop01:9092 --from-beginning --topic first

kafka-console-consumer --bootstrap-server hadoop01:9092 --from-beginning --topic first   # --from-beginning：会把主题中以往所有的数据都读取出来。

# 查看某个Topic的详情
kafka-topics --zookeeper hadoop01:2181/kafka --describe --topic first

# 修改分区数
kafka-topics --zookeeper hadoop01:2181/kafka --alter --topic first --partitions 6

```

# Flink

## 特别提示

### 设置 checkpoint

> flink提交iceberg的信息是在每次checkpoint的时候提交的。在sql client配置checkpoint的方法如下：

> 在flink-conf.yaml添加如下配置

```bash
execution.checkpointing.interval: 10s   # checkpoint间隔时间
execution.checkpointing.tolerable-failed-checkpoints: 10  # checkpoint 失败容忍次数
```

## SQL Client 启动

```bash
#   启动 flink
./bin/start-cluster.sh

# 启动 sql client, 添加hive 连接jar
./bin/sql-client.sh embedded -j /opt/flink-1.11.2/lib/iceberg-flink-runtime-0.11.1.jar -j /opt/flink-1.11.2/lib/flink-sql-connector-hive-2.2.0_2.11-1.11.2.jar -j /opt/flink-1.11.2/lib/flink-sql-connector-kafka_2.11-1.11.2.jar shell

# jar 放入 flink/lib
./bin/sql-client.sh embedded shell
```

### BUG解决

#### 字节校验问题

> sql-client 启动中字节校验的问题

```bash

# check if SQL client is already in classpath and must not be shipped manually
if [[ "$CC_CLASSPATH" =~ .*flink-sql-client.*.jar ]]; then

    # start client without jar
    exec $JAVA_RUN $JVM_ARGS -noverify   "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.table.client.SqlClient "$@"

# check if SQL client jar is in /opt
elif [ -n "$FLINK_SQL_CLIENT_JAR" ]; then

    # start client with jar
    exec $JAVA_RUN $JVM_ARGS -noverify  "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS:$FLINK_SQL_CLIENT_JAR"`" org.apache.flink.table.client.SqlClient "$@" --jar "`manglePath $FLINK_SQL_CLIENT_JAR`"
```

## Catalog创建

### hive catalog

```sql
CREATE
CATALOG hive WITH (
     'type'='hive',
     'property-version'='1',
     'hive-version'='3.1.2',
     'hive-conf-dir'='/etc/hive/conf.cloudera.hive'
);

```

### jdbc catalog

```sql
-- create a catalog which gives access to the backing Postgres installation
CREATE
CATALOG postgres WITH (
    'type'='jdbc',
    'property-version'='1',
    'base-url'='jdbc:postgresql://postgres:5432/',
    'default-database'='postgres',
    'username'='postgres',
    'password'='example'
);
```

### iceberg catalog

#### hive catalog

```sql

-- 创建 hive catalog
CREATE
CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://hadoop01:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://hadoop01:8020/warehouse/path'
);

```

#### hadoop catalog

```sql
-- 创建 hadoop catalog
CREATE
CATALOG iceberg_hadoop WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://hadoop01:8020/warehouse/path',
  'property-version'='1'
);

```

## 配置文件定义Catalog

> 如果不想每次启动sql client都重新执行ddl，可以在sql-client-defaults.yaml

```bash
catalogs: # empty list
# A typical catalog definition looks like:
  - name: hive
    type: hive
    hive-conf-dir: /etc/hive/conf.cloudera.hive
    hive-version: 3.1.2
    property-version: 1
  - name: iceberg_hive
    type: iceberg
    warehouse: hdfs://hadoop01:8020/warehouse/hive/iceberg # 表数据存储位置
    uri: thrift://hadoop01:9083
    catalog-type: hive # 使用 hive 保存表的 matedata
    property-version: 1
    clients: 5
  - name: iceberg_hadoop
    type: iceberg
    warehouse: hdfs://hadoop01:8020/warehouse/hadoop/iceberg
    catalog-type: hadoop
    property-version: 1
```

> **hive catalog 和iceberg-catalog-hive 是一样的，都是把matedata存储在 hive中**

## Catalog 使用

> 示例

```sql
use
catalog hive_catalog;

CREATE
DATABASE iceberg;

USE
iceberg;


CREATE TABLE iceberg_hadoop.iceberg.sample
(
    id   BIGINT COMMENT 'unique id',
    data STRING
);
```

# Iceberg

## 特别提示

### iceberg catalog表

> 不要在iceberg catalog下创建非iceberg table

> 我们在CREATE CATALOG iceberg创建iceberg catalog，然后使用use catalog iceberg之后，不要在这里创建非iceberg的table，这时候会出现不报错，但是也写不进去数据的情况。

> 也就是 iceberg_catalog 只创建 iceberg 表

# Kafka入库Iceberg实操

## 启动 flink 集群

```bash
./bin/start-cluster.sh
```

## 创建表

### 创建Iceberg表

```sql

-- 创建 hive catalog； 强依赖于 hive metadata 、mysql
CREATE
CATALOG iceberg_hive WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://hadoop01:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://hadoop01:8020/warehouse/hive/iceberg'
);

-- 创建 hadoop catalog，只依赖于hdfs（推荐）
CREATE
CATALOG iceberg_hadoop WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://hadoop01:8020/warehouse/hadoop/iceberg',
  'property-version'='1'
);

use
catalog iceberg_hadoop;

create
database  IF NOT EXISTS iceberg_hadoop.iceberg;

use
iceberg_hadoop.iceberg;

CREATE TABLE iceberg_hadoop.iceberg.ncddzt
(
    SOURCE_TYPE STRING,
    `INDEX`     STRING,
    SOURCE_HOST STRING,
    TOPIC       STRING,
    FILE_PATH   STRING,
    `POSITION`  STRING,
    LOG         STRING
    --proctime AS PROCTIME()\n" +
);


CREATE TABLE ods_ncddzt
(
    source_type       STRING,
    `index`           STRING,
    `agent_timestamp` STRING,
    source_host       STRING,
    topic             STRING,
    num               INT,
    file_path         STRING,
    `position`        STRING,
    log               STRING
) PARTITIONED BY (topic) WITH (
    'type'='ICEBERG',
    'iceberg.format.version'='2',
    'engine.hive.enabled'='true',
    'read.split.target-size'='1073741824',
    'write.format.default'='parquet',
    'write.distribution-mode'='hash',
    'write.metadata.delete-after-commit.enable'='true',
    'write.metadata.previous-versions-max'='2',
    'partition.bucket.source'='index',
    'partition.bucket.num'='10',
    'write.sink.mode'='copy-on-write'
)


```

### 创建Kafka表

```sql
-- kafka table 
CREATE
CATALOG hive WITH (
     'type'='hive',
     'property-version'='1',
     'hive-version'='3.1.2',
     'hive-conf-dir'='/etc/hive/conf.cloudera.hive'
);

use
catalog hive;

CREATE
DATABASE IF NOT EXISTS kafka;

use
kafka;

CREATE TABLE ncddzt
(
    SOURCE_TYPE STRING,
    `INDEX`     STRING,
    SOURCE_HOST STRING,
    TOPIC       STRING,
    FILE_PATH   STRING,
    `POSITION`  STRING,
    LOG         STRING,
    --AGENT_TIMESTAMP TIMESTAMP(3),
    proctime AS PROCTIME() -- generates processing-time attribute using computed column
    --WATERMARK FOR AGENT_TIMESTAMP AS AGENT_TIMESTAMP - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute
) WITH (
      'connector' = 'kafka', -- using kafka connector
      'topic' = 'iceberg', -- kafka topic
      'properties.group.id' = 'my_group', -- reading from the beginning
      'scan.startup.mode' = 'latest-offset', -- earliest-offset
      'properties.bootstrap.servers' = 'hadoop01:9092', -- kafka broker address
      'format' = 'json' -- the data format is json
      );
```

## 数据入库

```sql
INSERT INTO iceberg_hadoop.iceberg.ncddzt
SELECT SOURCE_TYPE, `INDEX`, SOURCE_HOST, TOPIC, FILE_PATH, `POSITION`, LOG
FROM hive.kafka.ncddzt;
```

## 入库数据查询

```sql
-- 读
-- Submit the flink job in streaming mode for current session.
SET
execution.type = streaming ;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET
table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
SELECT *
FROM iceberg_hadoop.iceberg.ncddzt /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

-- Read all incremental data starting from the snapshot-id '3821550127947089987' (records from this snapshot will be excluded).
SELECT *
FROM iceberg_hadoop.iceberg.ncddzt /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;

```


# 其他

## CDH路径结构说明

```bash
# 各种应用的shell客户端
cd /opt/cloudera/parcels/CDH/bin

# 各种应用的安装位置 
cd /opt/cloudera/parcels/CDH/lib

# 安装各个组件目录 
cd /opt/cloudera/parcels/

# 各种应用的日志目录 
cd /var/log

# 各种应用的配置文件目录
# cm agent的配置目录
cd /etc/cloudera-scm-agent/config.ini
#  cm server的配置目录
cd /etc/cloudera-scm-server/
# Hadoop各个组件的配置
cd /etc/hadoop/conf
# hive配置文件目录
cd /etc/hive/conf
#  hbase配置文件目录
cd /etc/hbase/conf
# 是hadoop集群以及组件的配置文件文件夹
cd /opt/cloudera/parcels/CDH/etc

# 服务运行时所有组件的配置文件目录
cd /var/run/cloudera-scm-agent/process

# jar包目录
# 所有jar包所在目录 
cd /opt/cloudera/parcels/CDH/jars
# 各个服务组件对应的jar包
cd /opt/cloudera/parcels/CDH/lib/

# Parcels包目录
# 服务软件包数据(parcels)
cd /opt/cloudera/parcel-repo/
# 服务软件包缓存数据
cd /opt/cloudera/parcel-cache/

```

# Hadoop

```bash
# 切换用户  sudo -u 用户名  command
sudo -u hdfs  hdfs dfs -chown root:supergroup  /warehouse/iceberg/iceberg_db
sudo -u hdfs hdfs dfs -rm -skipTrash -r /warehouse/iceberg/*
```