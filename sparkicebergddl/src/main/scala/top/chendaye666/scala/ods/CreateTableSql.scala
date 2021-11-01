package top.chendaye666.scala.ods

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 使用 Sql 创建表
 * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
 */
object CreateTableSql {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
    conf.set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop01:8020/warehouse/iceberg")

    val spark = SparkSession
      .builder()
      .config(conf)
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sql =
      """
        |CREATE TABLE hadoop_prod.t1.test21 (id bigint, data string)
        |USING iceberg
        |PARTITIONED BY (id)
        |TBLPROPERTIES  (
        | 'write.metadata.delete-after-commit.enabled'='true',
        | 'write.metadata.previous-versions-max'='6'
        |)
        |""".stripMargin

    spark.sql(sql)
    spark.sql("INSERT INTO hadoop_prod.t1.test21 VALUES (2, '1')")

    spark.close()
  }
}
