package top.chendaye666.timedtask.spark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

/**
 * 压缩数据(小)文件
 * https://cloud.tencent.com/developer/article/1770789
 * <p>
 * 指定hadoop-catalog
 * https://github.com/apache/iceberg/issues/3731
 */
public class CompactDataFilesAction {
    public static void main(String[] args) throws NoSuchTableException, ParseException {
        SparkConf sparkConf = new SparkConf()
                .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
                .set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop01:8020/warehouse/iceberg")
                .set("spark.sql.catalog.hadoop_prod.default-catalog", "realtime")
//                .setMaster("local[*]")
                .setAppName("compact file");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        ParameterTool params = ParameterTool.fromArgs(args);
        String name = params.get("table", null);
        // hadoop_prod.realtime.ncdd_raw
        Table table = Spark3Util.loadIcebergTable(sparkSession, name);

        /**
         * https://iceberg.apache.org/javadoc/0.13.1/org/apache/iceberg/actions/RewriteDataFiles.html
         * https://www.cnblogs.com/payapa/p/15932512.html
         *
         * https://iceberg.apache.org/javadoc/0.13.1/constant-values.html#org.apache.iceberg.actions.RewriteDataFiles.TARGET_FILE_SIZE_BYTES
         *
         * Exception in thread "main" java.lang.RuntimeException:
         * Cannot commit rewrite because of a ValidationException or CommitFailedException.
         * This usually means that this rewrite has conflicted with another concurrent Iceberg operation.
         * To reduce the likelihood of conflicts, set partial-progress.enabled which will
         * break up the rewrite into multiple smaller commits controlled by partial-progress.max-commits.
         * Separate smaller rewrite commits can succeed independently while any commits that conflict
         * with another Iceberg operation will be ignored. This mode will create additional snapshots in the table history,
         * one for each commit.
         */
        SparkActions
                .get(sparkSession)
                .rewriteDataFiles(table)
//                .filter(Expressions.equal("date", "2020-08-18"))
                .option("target-file-size-bytes", Long.toString(500 * 1024 * 1024)) // 500 MB
                .option("partial-progress.enabled", "true") // 500 MB
//                .option("use-starting-sequence-number", "true") // 500 MB
                .execute();
        sparkSession.close();
    }
}

