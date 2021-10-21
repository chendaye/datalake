package top.chendaye666.rewrite;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

/**
 * 在 Flink Iceberg Sink 中支持 write.distribution-mode=hash 的方式写入数据，这可以从生产源头上大量减少小文件
 * 定时任务，压缩小文件
 * ./bin/flink run /opt/work/datalake-1.0-SNAPSHOT.jar
 * ./bin/flink run -t yarn-per-job --detached  /opt/work/task-1.0-SNAPSHOT.jar
 * ./bin/flink run  -m yarn-cluster -ynm rewrite-small-file -p 4  -ys 4 -yjm 1500 -ytm 1500  /opt/work/task-1.0-SNAPSHOT.jar
 */
public class RewriteFileAction {
  public static void main(String[] args) throws Exception {
//    System.setProperty("HADOOP_USER_NAME", "root");
    TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
    tableLoader.open();
    Table table = tableLoader.loadTable();

    Actions.forTable(table)
            .rewriteDataFiles()
            .maxParallelism(3)
//            .filter(Expressions.equal("day", day))
            .targetSizeInBytes(100 * 1024 * 1024)
            .execute();
  }
}
