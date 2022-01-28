package realtime.service;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommonService {
    public void createHadoopCatalog(StreamTableEnvironment tEnv){
        String hadoopCatalogSql = "CREATE CATALOG hadoop_prod WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/iceberg',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.executeSql(hadoopCatalogSql);
    }
}
