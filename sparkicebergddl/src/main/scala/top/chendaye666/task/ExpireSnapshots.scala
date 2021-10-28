package top.chendaye666.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 删除过期快照
 * Using those SQL commands requires adding Iceberg extensions to your Spark environment using the following
 * https://iceberg.apache.org/spark-procedures/#expire_snapshots
 */
object ExpireSnapshots {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.hadoop_prod","org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.hadoop_prod.type","hadoop")
    conf.set("spark.sql.catalog.hadoop_prod.warehouse","hdfs://hadoop01:8020/warehouse/iceberg")
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // CALL hive_prod.system.expire_snapshots('db.sample', date_sub(current_date(), 10), 100)
    // 删除过去 10 天的快照，并且保留最新的 100 个快照
    val procedure =
      """
        |CALL hadoop_prod.system.expire_snapshots('t1.test', date_sub(current_date(), 10), 100)
        |""".stripMargin

    // Erase all snapshots older than the current timestamp but retain the last 5 snapshots
    // 删除当前的所有快照，但是保留最新的 5 个快照
    val currentTimestamp = System.currentTimeMillis()
    val procedure1 =
      s"""
        |CALL hadoop_prod.system.expire_snapshots('t1.test', ${currentTimestamp}, 5)
        |""".stripMargin

    spark.sql(procedure1)
    spark.close()
  }

}
