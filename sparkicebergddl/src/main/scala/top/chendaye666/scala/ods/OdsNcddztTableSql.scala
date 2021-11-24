package top.chendaye666.scala.ods

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * spark 创建 ncddzt ods表
 */
object OdsNcddztTableSql {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val conf = new SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
    conf.set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop01:8020/warehouse/iceberg")

    val spark = SparkSession
      .builder()
      .config(conf)
      .appName(this.getClass.getSimpleName)
//      .master("local[*]")
      .master("spark://hadoop01:7077")
      .getOrCreate()

    val sql =
      """
        |CREATE TABLE IF NOT EXISTS  hadoop_prod.ods.ncddzt (
        | source_type string,
        | index string,
        | agent_timestamp string,
        | source_host string,
        | topic string,
        | num int,
        | file_path string,
        | position string,
        | log string
        |)
        |USING iceberg
        |PARTITIONED BY (topic)
        |TBLPROPERTIES  (
        | 'write.metadata.delete-after-commit.enabled'='true',
        | 'write.metadata.previous-versions-max'='6',
        | 'read.split.target-size'='1073741824'
        |)
        |""".stripMargin

    spark.sql(sql)
    spark.close()
  }
}
