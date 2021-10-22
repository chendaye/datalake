package top.chendaye666.flink;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;

import java.util.concurrent.TimeUnit;

/**
 * iceberg 本身的架构设计决定了，对于实时入湖场景，会产生大量的 snapshot 文件，
 * 快照过期策略是通过额外的定时任务周期执行，过期 snapshot 文件和过期数据文件均会被删除。
 * 如果实际使用场景不需要 time travel 功能，则可以保留较少的 snapshot 文件
 *
 * https://www.163.com/dy/article/GC8GAT0A0511FQO9.html
 */
public class SnapshotAction {
    public static void main(String[] args) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");
        tableLoader.open();
        Table table = tableLoader.loadTable();
        Snapshot snapshot = table.currentSnapshot();
        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
        long tsToExpire2 = snapshot.timestampMillis() - TimeUnit.MINUTES.toMillis(60); // 过去5分钟
        table.expireSnapshots().expireOlderThan(tsToExpire2).commit();

    }
}
