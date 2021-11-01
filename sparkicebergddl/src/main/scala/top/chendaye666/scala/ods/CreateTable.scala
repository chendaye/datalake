package top.chendaye666.scala.ods

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession

object CreateTable {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    // 创建 SparkSession 设置Hadoop Catalog
    val sparkSession = SparkSession
      .builder()
      //      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") // 设置扩展，支持 merge into 等功能
      //      .config("spark.sql.catalog.hadoop_test", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.hadoop_test.type", "hadoop") // 设置数据源类别为hadoop
      //      .config("spark.sql.catalog.hadoop_test.warehouse", "hdfs://hadoop01:8020/warehouse/path") // 设置数据源位置(本地)
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val conf = new Configuration()
    val warehousePath = "hdfs://hadoop01:8020/warehouse/iceberg"
    val catalog = new HadoopCatalog(conf, warehousePath)
    // 数据库 表
    val name = TableIdentifier.of("t1", "test3")
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.StringType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "age", Types.IntegerType.get()),
    )
    // 创建表
    catalog.createTable(name, schema)

    sparkSession.close()
  }
}
