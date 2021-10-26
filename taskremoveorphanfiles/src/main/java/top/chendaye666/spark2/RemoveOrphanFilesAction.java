package top.chendaye666.spark2;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.sql.SparkSession;

/**
 * 清除无用文件
 */
public class RemoveOrphanFilesAction {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Table table = tables.load("hdfs://hadoop01:8020/warehouse/path/ods/ods_ncddzt");

        Actions.forTable(table)
                .removeOrphanFiles()
                .execute();

//        sparkSession.close();
    }
}
