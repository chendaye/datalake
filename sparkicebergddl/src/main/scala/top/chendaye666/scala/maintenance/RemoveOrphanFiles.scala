package top.chendaye666.scala.maintenance

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * remove_orphan_files
 */
object RemoveOrphanFiles {
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

    // Used to remove files which are not referenced in any metadata files of an Iceberg table and can thus be considered “orphaned”.
    val procedure =
    """
      |CALL hadoop_prod.system.remove_orphan_files(table => 't1.test', dry_run => true)
      |""".stripMargin

    spark.sql(procedure)
    spark.close()
  }
}
