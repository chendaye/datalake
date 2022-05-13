package top.chendaye666.timedtask.iceberg;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/**
 * https://iceberg.apache.org/docs/latest/maintenance/
 * todo: 删除过期快照
 *      Expiring old snapshots removes them from metadata, so they are no longer available for time travel queries.
 */
public class ExpireSnapshotsActions {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);

        ParameterTool params = ParameterTool.fromArgs(args);
        String name = params.get("table",     null);
        // hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general
        Table table = tables.load(name);

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 10); // 1h
        table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .commit();
    }

    /**
     * 使用SparkActions，删除过期的快照文件
     */
    public static void sparkActions(){
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general");

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 1); // 1h
        SparkActions
                .get()
                .expireSnapshots(table)
                .expireOlderThan(tsToExpire)
                .execute();

        sparkSession.close();
    }
}
