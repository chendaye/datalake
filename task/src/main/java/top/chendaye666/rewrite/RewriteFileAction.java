package top.chendaye666.rewrite;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

/**
 * 定时任务，压缩小文件
 * ./bin/flink run /opt/work/datalake-1.0-SNAPSHOT.jar
 * ./bin/flink run -t yarn-per-job --detached  /opt/work/datalake-1.0-SNAPSHOT.jar
 */
public class RewriteFileAction {
  public static void main(String[] args) throws Exception {
    System.setProperty("HADOOP_USER_NAME", "root");
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
    Table table = tableLoader.loadTable();
    RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .targetSizeInBytes(100 * 1024 * 1024)
        .execute();
  }
}
