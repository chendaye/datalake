package top.chendaye666.create;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;

/**
 * 删除孤立文件
 */
public class RemoveOrphanFiles {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("RemoveOrphanFiles")
                .master("local[*]")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/iceberg";
        HadoopCatalog catalog1 = new HadoopCatalog(conf, warehousePath);
        Table table = catalog1.loadTable(TableIdentifier.of("t1", "test"));

        Actions.forTable(table)
                .removeOrphanFiles()
                .execute();
        sparkSession.close();
    }
}
