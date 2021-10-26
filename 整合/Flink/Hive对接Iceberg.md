

# Iceberg To Hive


## iceberg 配置 hive 支持

> Hadoop configuration 全局配置


> Warning:在0.11.x版本中，如果Hive使用Tez引擎，必须设置 `hive.vectorized.execution.enabled=false`

在 `hive-site.xml` 配置 `iceberg.engine.hive.enabled=true`

> Table property configuration 局部配置

```java
Catalog catalog = ...;
Map<String, String> tableProperties = Maps.newHashMap();
tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
catalog.createTable(tableId, schema, spec, tableProperties);
```

## catalog 管理

> 对hive来说只有 一个全局 catalog， 定义在Hadoop configuration

> iceberg 有多种 catalog 类型：hive、hadoop、自定义； 还支持直接从 文件路径 加载表（`path-based tables`），表不属于任何catalog

> 如何用 hive engine 读取这些交叉的表（`cross-catalog and path-based tables`），比如 join
> hive 在metadata 有3中不通的方式，表示 iceberg table。
> 不通的方式取决于 表的 `iceberg.catalog` 属性

- ①：如果没有配置`iceberg.catalog`, 会使用 `HiveCatalog(metastore configured in the Hive environment )`加载表
- ②：如果配置了`iceberg.catalog`，就会使用配置的 catalog，加载表
- ③：如果`iceberg.catalog`是设置为`location_based_table`,会直接使用 表的根路径（`table’s root location`）加载表

> 对于情况②、③，用户可以再hive的metadata中创建一个 iceberg table的视图；这样不通类型的表就可以在同一个hive环境中一起工作

[CREATE EXTERNAL TABLE ](https://iceberg.apache.org/hive/#create-external-table)
[CREATE TABLE](https://iceberg.apache.org/hive/#create-table)



### Custom Iceberg catalogs

> 要全局注册不同的目录，请设置以下 Hadoop 配置：


|  Config Key   | Description  |
|  ----  | ----  |
| iceberg.catalog.<catalog_name>.type  | type of catalog: hive or hadoop |
| iceberg.catalog.<catalog_name>.catalog-impl  | catalog implementation, must not be null if type is null |
| iceberg.catalog.<catalog_name>.<key>  | any config key and value pairs for the catalog |



> Hive CLI 使用案例

```sql
# Register a HiveCatalog called another_hive:

SET iceberg.catalog.another_hive.type=hive;
SET iceberg.catalog.another_hive.uri=thrift://example.com:9083;
SET iceberg.catalog.another_hive.clients=10;
SET iceberg.catalog.another_hive.warehouse=hdfs://example.com:8020/warehouse;

#Register a HadoopCatalog called hadoop:

SET iceberg.catalog.hadoop.type=hadoop;
SET iceberg.catalog.hadoop.warehouse=hdfs://example.com:8020/warehouse;

# Register an AWS GlueCatalog called glue:

SET iceberg.catalog.glue.catalog-impl=org.apache.iceberg.aws.GlueCatalog;
SET iceberg.catalog.glue.warehouse=s3://my-bucket/my/key/prefix;
SET iceberg.catalog.glue.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager;
SET iceberg.catalog.glue.lock.table=myGlueLockTable;

```

## 创建Hive表

### 创建外部表

> 创建外部表作为已存在的iceberg table的视图

> Iceberg 表是使用 Catalog 或 Tables 接口的实现创建的，需要相应地配置 Hive 以对这些不同类型的表进行操作。


#### Catalog

##### Hive catalog tables

> 如果 iceberg table 使用 hive catalog创建，则hive engine 直接可见，不需要再创建一个视图


##### Custom catalog tables

> 对于一个已经注册的非 hive catalog 如 hadoop catalog；要在sql 中声明 `iceberg.catalog`

> 下面的例子为 名为 hadoop_cat 的 hadoop catalog 创建一个 视图 

```sql
SET iceberg.catalog.hadoop_cat.type=hadoop;
SET iceberg.catalog.hadoop_cat.warehouse=hdfs://example.com:8020/hadoop_cat;

CREATE EXTERNAL TABLE database_a.table_a
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='hadoop_cat');
```

> 如果在 `table properties` 和`global Hadoop configuration` 中都没有配置 `iceberg.catalog` 就默认使用
> `HiveCatalog`


##### Path-based Hadoop tables

> 如果使用`HadoopTables`创建 `iceberg table`，这些表直接整个被存储在文件系统（如：HDFS）中。
> 这些表不属于任何 catalog。此时 `iceberg.catalog` 设置为`location_based_table`

```s
CREATE EXTERNAL TABLE table_a 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 'hdfs://some_bucket/some_path/table_a'
TBLPROPERTIES ('iceberg.catalog'='location_based_table')
```


### 创建内部表



> Hive 支持直接通过 `CREATE TABLE` 创建 `iceberg table`

```sql
CREATE TABLE database_a.table_a (
  id bigint, name string
) PARTITIONED BY (
  dept string
) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';
```

> NOTE:尽管底层的 `iceberg table` 有分区，但是Hive 表没有分区

> NOTE：由于Hive的 `PARTITIONED BY`语法限制，当前在Hive中只能对列分区（会被转换为iceberg的分区语义）。

> 不能使用iceberg的其他分区方式，比如 days(timestamp)，要是使用 iceberg的所有分区方式，需要在 spark 或者 flink中创建表


#### Custom catalog table


> 可以使用自定义 catalog去创建新 table
> 下面的例子使用 ` custom Hadoop catalog` 创建表

```sql
SET iceberg.catalog.hadoop_cat.type=hadoop;
SET iceberg.catalog.hadoop_cat.warehouse=hdfs://example.com:8020/hadoop_cat;

CREATE TABLE database_a.table_a (
  id bigint, name string
) PARTITIONED BY (
  dept string
) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='hadoop_cat');
```

> Warning:如果已经要创建的表已经存在于 `custom catalog`中，就会创建一个视图表（managed overlay table）

> 这意味着在创建视图表的时候可以省略关键字`EXTERNAL`

> 不建议这样做，因为在 Hive 端意外删除表命令的情况下，创建托管覆盖表可能会给共享数据文件带来风险，这会无意中删除表中的所有数据。


### 删除表

> 删除表的命令

```sql
DROP TABLE [IF EXISTS] table_name [PURGE];
```

> 可以通过 `global Hadoop configuration` or `Hive metastore table properties`配置 `purge` 模式


|Config key|Default	|Description|
|---|---|---|
|external.table.purge|true|if all data and metadata should be purged in a table by default|

> 每一个`iceberg table` 的 `purge`模式都可以在 `table properties`中配置



|Property|Default	|Description|
|---|---|---|
|gc.enabled|true|if all data and metadata should be purged in the table by default|


> 通过 UpdateProperties 更改 Iceberg 表上的 gc.enabled 时，也会相应地更新 HMS 表上的 external.table.purge。

> 在 Hive CREATE TABLE 期间将 external.table.purge 设置为 table prop 时，gc.enabled 会根据 Iceberg table properties 相应地下推。

> 这确保在 Hive 和 Iceberg 2个表级别上 2 个属性始终保持一致。



> 注意：使用 `Hive ALTER TABLE SET TBLPROPERTIES` 更改 `external.table.purge` 不会更新 `Iceberg table`的`gc.enabled`。这是因为 Hive 3.1.2的限制，没有所用用于修改表的钩子


## SQL查询

> 以下是 Iceberg Hive 读取支持的功能亮点

- Predicate pushdown: 已实现 Hive SQL WHERE 子句的下推，以便在 Iceberg TableScan 级别以及 Parquet 和 ORC 读取器使用这些过滤器。

- Column projection: Hive SQL SELECT 子句中的列向下投射到 Iceberg 读取器以减少读取的列数。

- Hive query engines: MapReduce 和 Tez 查询引擎都支持.

### 配置


> 以下是 `Hive Read`可以调整的配置 


|Config key|	Default|	Description|
|---|---|---|
|iceberg.mr.reuse.containers	|false	|if Avro reader should reuse containers|
|iceberg.mr.case.sensitive	|true|	if the query is case-sensitive|

> 您现在应该能够发出 Hive SQL SELECT 查询并查看从底层 Iceberg 表返回的结果，例如：

```sql
SELECT * from table_a;
```

### 写SQL

> 以下是 `Hive writer` 可以调整的配置（`Hadoop configurations`）



|Config key|	Default|	Description|
|---|---|---|
|iceberg.mr.commit.table.thread.pool.size|	10|	the number of threads of a shared thread pool to execute parallel commits for output tables|
|iceberg.mr.commit.file.thread.pool.size	|10|	the number of threads of a shared thread pool to execute parallel commits for files in each output table|


> INSERT INTO: Hive supports the standard single-table INSERT INTO operation

```sql
INSERT INTO table_a VALUES ('a', 1);
INSERT INTO table_a SELECT ...;
```

> 也支持多表插入，但它不是原子的，一次提交一张表。

> 部分更改将在提交过程中可见，失败可能会导致部分更改被提交。

> 单个表中的更改将保持原子性。

```sql
FROM customers
    INSERT INTO target1 SELECT customer_id, first_name
    INSERT INTO target2 SELECT last_name, customer_id;
```

## 类型兼容


> Hive 和 Iceberg支持不同的类型集合

> Iceberg可以自动进行类型转换，除了一些特殊的组合之外

> 所以你可能想了解在 Iceberg表字段的设计时期，类型如何转换

> 你可以开启 `iceberg.mr.schema.auto.conversion` 默认为 `false`

|Config key|	Default|	Description|
|---|---|---|
|iceberg.mr.schema.auto.conversion|	false|	if Hive should perform type auto-conversion|


### Hive Iceberg 类型对照

> 此类型转换表描述了 Hive 类型如何转换为 Iceberg 类型。

> 转换适用于创建 Iceberg 表和通过 Hive 写入 Iceberg 表。

|Hive|Iceberg|Noets|
|---|---|---|
|boolean|boolean||
|short|integer|auto-conversion|
|byte|integer|auto-conversion|
|integer|integer||
|long|long||
|float|float||
|double|double||
|date|date||
|timestamp|timestamp|without timezone|
|timestamplocaltz|timestamp with timezone|Hive 3 only|
|interval_year_month|not supported||
|interval_day_time|not supported||
|char|string|auto-conversion|
|varchar|string|auto-conversion|
|string|string||
|binary|binary||
|decimal|decimal||
|struct|struct||
|list|list||
|map|map||
|union|not supported||





# Hive 查询 Iceberg实操

## 启动Hive CLient

> Beeline 用法
Beeline 要与HiveServer2配合使用 服务端启动hiveserver2 ，客户的通过beeline两种方式连接到hive

```bash
# beelinebeeline> !connect jdbc:hive2://<host>:<port>/<db>;auth=noSasl root 123默认 用户名、密码不验证
beeline -u jdbc:hive2://hadoop01:10000/default -n root
```


## 依赖包

[iceberg-hive-runtime-0.11.1.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.11.1/iceberg-hive-runtime-0.11.1.jar)


> iceberg-hive-runtime-0.11.1.jar 放到 hive/lib 目录中

> 或者

```bash
add jar /opt/software/iceberg-hive-runtime-0.11.1.jar;
```




## 创建外部表

### Iceberg的hadoop catalog表

> 设置 catalog

```sql 
SET iceberg.catalog.hadoop_catalog.type=hadoop;
SET iceberg.catalog.hadoop_catalog.warehouse=hdfs://hadoop01:8020/warehouse/path; 
```

> 创建外部表

```sql
-- 例一
CREATE EXTERNAL TABLE ods.ods_ncddzt(
    source_type STRING,
   `index` STRING,
   `agent_timestamp` STRING,
   source_host STRING,
   topic STRING,
   num INT,
   file_path STRING,
   `position` STRING,
   log STRING)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
LOCATION '/warehouse/path/ods/ods_ncddzt'
TBLPROPERTIES ('iceberg.catalog'='hadoop_catalog');
```

```sql
-- 例二
CREATE EXTERNAL TABLE dwd.dwd_ncddzt(
   source_type STRING,
   `index` STRING,
   `agent_timestamp` STRING,
   source_host STRING,
   topic STRING,
   file_path STRING,
   `position` STRING,
   `time` BIGINT ,
   log_type String ,
   qd_number String ,
   seat String ,
   market String ,
   cap_acc String ,
   suborderno String ,
   wt_pnum String ,
   contract_num String 
) 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
LOCATION '/warehouse/path/dwd/dwd_ncddzt'
TBLPROPERTIES ('iceberg.catalog'='hadoop_catalog');

```


### 创建Iceberg 表视图

```sql
-- 创建 Hive 表视图
CREATE EXTERNAL TABLE ods_ncddzt(SOURCE_TYPE STRING,
    `INDEX` STRING,
    SOURCE_HOST STRING,
    TOPIC STRING,
    FILE_PATH STRING,
    `POSITION` STRING,
    LOG STRING) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' LOCATION '/warehouse/path/ods/ods_ncddzt';
select * from default.ods_ncddzt;

```



