package top.chendaye666.java.maintenance;

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
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CompactSmallFile")
//                .master("local[*]")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog1 = new HadoopCatalog(conf, warehousePath);
        Table table = catalog1.loadTable(TableIdentifier.of("ods", "ods_ncddzt"));
        Actions.forTable(table)
                .rewriteDataFiles()
//            .filter(Expressions.equal("day", day))
                .targetSizeInBytes(500 * 1024 * 1024)// 128mb
                .execute();

        sparkSession.close();
    }
}
