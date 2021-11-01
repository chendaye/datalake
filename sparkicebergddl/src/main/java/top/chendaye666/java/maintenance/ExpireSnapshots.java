package top.chendaye666.java.maintenance;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;

/**
 * 删除过期数据
 */
public class ExpireSnapshots {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ExpireSnapshots")
                .master("local[*]")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog1 = new HadoopCatalog(conf, warehousePath);
        Table table = catalog1.loadTable(TableIdentifier.of("ods", "ods_ncddzt"));

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
        Actions.forTable(table)
                .expireSnapshots()
                .expireOlderThan(tsToExpire)
                .execute();

        sparkSession.close();
    }
}
