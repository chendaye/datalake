package top.chendaye666.create;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;

/**
 * 合并小文件
 */
public class CompactSmallFile {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CompactSmallFile")
                .master("local[*]")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/iceberg";
        HadoopCatalog catalog1 = new HadoopCatalog(conf, warehousePath);
        Table table = catalog1.loadTable(TableIdentifier.of("t1", "test"));
        Actions.forTable(table)
                .rewriteDataFiles()
//            .filter(Expressions.equal("day", day))
                .targetSizeInBytes(500 * 1024 * 1024)// 128mb
                .execute();

        sparkSession.close();
    }
}
