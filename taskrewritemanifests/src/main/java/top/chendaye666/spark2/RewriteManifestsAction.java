package top.chendaye666.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.SparkSession;

/**
 * 重写（manifests 文件）
 */
public class RewriteManifestsAction {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

//        Configuration conf = new Configuration();
//        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
//        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
//        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ods"), "ods_ncddzt");
//        Table table = catalog.loadTable(tableIdentifier);

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_raw");

        table.rewriteManifests()
                .rewriteIf(file -> file.length() < 20 * 1024 * 1024) // 10 MB
                .clusterBy(file -> file.partition().get(0, Integer.class))
                .commit();

//        sparkSession.close();
    }
}
