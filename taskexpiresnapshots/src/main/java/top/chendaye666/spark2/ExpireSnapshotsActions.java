package top.chendaye666.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * todo: 运行成功 ：删除过期快照
 */
public class ExpireSnapshotsActions {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://hadoop01:8020/warehouse/iceberg/realtime/ncdd_general");

        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 1); // 1h
        table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .commit();
    }
}
