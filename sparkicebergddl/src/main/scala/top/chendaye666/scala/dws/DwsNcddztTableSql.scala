package top.chendaye666.scala.dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * spark 创建 ncddzt dwd表
 */
object DwsNcddztTableSql {
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
        |CREATE TABLE IF NOT EXISTS  hadoop_prod.dws.ncddzt (
        | source_type string,
        | agent_timestamp string,
        | topic  string,
        | total bigint
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
