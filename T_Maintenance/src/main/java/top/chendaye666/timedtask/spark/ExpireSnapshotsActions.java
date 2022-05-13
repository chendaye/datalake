package top.chendaye666.timedtask.spark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;

/**
 * todo:删除过期快照
 */
public class ExpireSnapshotsActions {
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

        // 删除时间，默认删除 72h 之前的数据（3天）
        String expire = params.get("expire",     "72"); // h
        long tsToExpire = System.currentTimeMillis() - 1000 * 60 * 60 * Long.parseLong(expire); // ms

        // hadoop_prod.realtime.ncdd_raw
        Table table = Spark3Util.loadIcebergTable(sparkSession, name);

        SparkActions
                .get(sparkSession)
                .expireSnapshots(table)
                .expireOlderThan(tsToExpire)
                .execute();

        sparkSession.close();
    }
}
