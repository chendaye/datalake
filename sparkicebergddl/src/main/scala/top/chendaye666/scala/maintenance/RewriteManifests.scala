package top.chendaye666.scala.maintenance

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * rewrite_manifests
 * Rewrite manifests for a table to optimize scan planning.
 */
object RewriteManifests {
  def main(args: Array[String]): Unit = {
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

      // Rewrite the manifests in table db.sample and align manifest files with table partitioning.
      val procedure =
        """
          |CALL hadoop_prod.system.rewrite_manifests('t1.test')
          |""".stripMargin

      // Rewrite the manifests in table db.sample and disable the use of Spark caching. This could be done to avoid memory issues on executors.
      val procedure1 =
        """
          |CALL hadoop_prod.system.rewrite_manifests('t1.test', false)
          |""".stripMargin

      spark.sql(procedure1)
      spark.close()
    }
  }
}
