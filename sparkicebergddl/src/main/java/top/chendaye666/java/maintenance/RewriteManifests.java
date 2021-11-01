package top.chendaye666.java.maintenance;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;

/**
 * 重写 manifests
 */
public class RewriteManifests {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("RewriteManifests")
                .master("local[*]")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog1 = new HadoopCatalog(conf, warehousePath);
        Table table = catalog1.loadTable(TableIdentifier.of("ods", "ods_ncddzt"));

        table.rewriteManifests()
                .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
//                .clusterBy(file -> file.partition().get(0, Integer.class))
                .commit();

        sparkSession.close();
    }
}
