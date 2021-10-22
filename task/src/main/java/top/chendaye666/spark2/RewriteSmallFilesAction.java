package top.chendaye666.spark2;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import org.apache.spark.sql.SparkSession;
//import  org.apache.iceberg.actions.SparkActions

/**
 * 压缩小文件
 */
public class RewriteSmallFilesAction {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
//                .master("local")
                .appName("rewrite")
                .getOrCreate();

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ods"), "ods_ncddzt");
        Table table = catalog.loadTable(tableIdentifier);

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
        Actions.forTable(sparkSession, table)
                .expireSnapshots()
                .expireOlderThan(tsToExpire)
                .execute();

        sparkSession.close();
    }
}

/*
spark-submit --class top.chendaye666.spark2.RewriteSmallFilesAction \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        --queue thequeue \
        /opt/work/task-1.0-SNAPSHOT.jar
*/
