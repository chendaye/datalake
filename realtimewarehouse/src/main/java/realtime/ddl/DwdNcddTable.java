package realtime.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建 dwd_ncddzt 表
 */
public class DwdNcddTable {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.executeSql(hadoopCatalogSql);

        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.dwd_ncddzt (\n" +
                "    `source_type` STRING,\n" +
                "    `index` STRING,\n" +
                "    `agent_timestamp` STRING,\n" +
                "    `source_host` STRING,\n" +
                "    `topic` STRING,\n" +
                "    `file_path` STRING,\n" +
                "    `position` STRING,\n" +
                "    `time` BIGINT,\n" +
                "    `log_type` STRING,\n" +
                "    `qd_number` STRING,\n" +
                "    `seat` STRING,\n" +
                "    `market` STRING,\n" +
                "    `cap_acc` STRING,\n" +
                "    `suborderno` STRING,\n" +
                "    `wt_pnum` STRING,\n" +
                "    `contract_num` STRING\n" +
                ") PARTITIONED BY (`topic`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }
}
