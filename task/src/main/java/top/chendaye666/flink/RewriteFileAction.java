package top.chendaye666.flink;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * 在 Flink Iceberg Sink 中支持 write.distribution-mode=hash 的方式写入数据，这可以从生产源头上大量减少小文件
 * 定时任务，压缩小文件
 * ./bin/flink run /opt/work/datalake-1.0-SNAPSHOT.jar
 * ./bin/flink run -t yarn-per-job --detached  /opt/work/task-1.0-SNAPSHOT.jar
 * ./bin/flink run  -m yarn-cluster -ynm rewrite-small-file -p 4  -ys 4 -yjm 1500 -ytm 1500  /opt/work/task-1.0-SNAPSHOT.jar
 *
 * https://iceberg.apache.org/java-api-quickstart/#create-a-table
 * https://blog.csdn.net/M283592338/article/details/120769331
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/ops/deployment/yarn_setup.html
 */
public class RewriteFileAction {
  public static void main(String[] args) throws Exception {
//    System.setProperty("HADOOP_USER_NAME", "root");

    Table table = loadTable01();
    Actions.forTable(table)
            .rewriteDataFiles()
            .maxParallelism(3)
//            .filter(Expressions.equal("day", day))
            .targetSizeInBytes(128 * 1024 * 1024)// 100mb
            .execute();
  }

  /**
   * 使用 TableLoader.fromHadoopTable
   * @return
   */
  private static Table loadTable01(){
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
    tableLoader.open();
    Table table = tableLoader.loadTable();
    return table;
  }

  /**
   * 使用 catalog
   * @return
   */
  private static Table loadTable02(){
    Configuration conf = new Configuration();
    String warehousePath = "hdfs://hadoop01:8020/warehouse/path";
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("ods"), "ods_ncddzt");
    Table table = catalog.loadTable(tableIdentifier);
    return table;
  }

  /**
   * 使用 table
   * @return
   */
  private static Table loadTable03(){
    Configuration conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    //    Table table = tables.create(schema, spec, table_location);
    // or to load an existing table, use the following line
   Table table = tables.load("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
   return table;
  }
}
