package top.chendaye666.spark2;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.SparkSession;

/**
 * 压缩小文件
 */
public class RewriteSmallFilesAction {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .getOrCreate();

//        Configuration conf = new Configuration();
//        HadoopTables tables = new HadoopTables(conf);
//        Table table = tables.load("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");

        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ods"), "ods_ncddzt");
        Table table = catalog.loadTable(tableIdentifier);

        Actions.forTable(table)
                .rewriteDataFiles()
//            .filter(Expressions.equal("day", day))
                .targetSizeInBytes(500 * 1024 * 1024)// 128mb
                .execute();

//        sparkSession.close();
    }
}

