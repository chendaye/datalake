package top.chendaye666.Service;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WarehouseTableService {
    /**
     * catalog
     * @param tEnv
     */
    public void createHadoopCatalog(StreamTableEnvironment tEnv) {
        String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.executeSql(hadoopCatalogSql);
    }

    /**
     * 一般表
     * @param tEnv
     */
    public void createCommonTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.ncdd_common (\n" +
                "    `mi` STRING,\n" +
                "    `time` STRING,\n" +
                "    `date` STRING,\n" +
                "    `node` STRING,\n" +
                "    `channel` STRING,\n" +
                "    `channel2` STRING,\n" +
                "    `channel3` STRING,\n" +
                "    `channel4` STRING,\n" +
                "    `channel5` STRING,\n" +
                "    `channel6` STRING,\n" +
                "    `val` FLOAT,\n" +
                "    `val_str` STRING\n" +
                ") PARTITIONED BY (`date`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }

    public void createGtulogTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.ncdd_gtulog (\n" +
                "    `mi` STRING,\n" +
                "    `time` STRING,\n" +
                "    `date` STRING,\n" +
                "    `node` STRING,\n" +
                "    `channel` STRING,\n" +
                "    `channel2` STRING,\n" +
                "    `channel3` STRING,\n" +
                "    `channel4` STRING,\n" +
                "    `channel5` STRING,\n" +
                "    `channel6` STRING,\n" +
                "    `val` FLOAT,\n" +
                "    `val_str` STRING\n" +
                ") PARTITIONED BY (`date`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }
}
