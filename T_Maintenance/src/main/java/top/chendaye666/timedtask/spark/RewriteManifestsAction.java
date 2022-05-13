package top.chendaye666.timedtask.spark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

/**
 * todo: 重写（manifests 文件）
 */
public class RewriteManifestsAction {
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
        String name = params.get("table",     null);
        // hadoop_prod.realtime.ncdd_raw
        Table table = Spark3Util.loadIcebergTable(sparkSession, name);

        SparkActions
                .get(sparkSession)
                .rewriteManifests(table)
                .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
                .execute();

        sparkSession.close();
    }
}
