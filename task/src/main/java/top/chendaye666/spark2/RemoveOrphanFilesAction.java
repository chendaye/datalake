package top.chendaye666.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * 清除无用文件
 */
public class RemoveOrphanFilesAction {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ods"), "ods_ncddzt");
        Table table = catalog.loadTable(tableIdentifier);

        Actions.forTable(table)
                .removeOrphanFiles()
                .execute();
    }
}
