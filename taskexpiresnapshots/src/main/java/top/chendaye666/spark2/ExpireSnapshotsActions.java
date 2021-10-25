package top.chendaye666.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.SparkSession;

/**
 * 删除过期快照
 */
public class ExpireSnapshotsActions {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
        Actions.forTable(table)
                .expireSnapshots()
                .expireOlderThan(tsToExpire)
                .execute();

        sparkSession.close();
    }
}
