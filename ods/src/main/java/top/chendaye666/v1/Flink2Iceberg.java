package top.chendaye666.v1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink2Iceberg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        String hadoopCatalogSql = "CREATE CATALOG iceberg WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://hadoop01:8020/warehouse/test',\n" +
                "  'property-version'='1'\n" +
                ")";

        tenv.executeSql(hadoopCatalogSql);

        tenv.useCatalog("iceberg");
        tenv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db");
        tenv.useDatabase("iceberg_db");

        tenv.executeSql("DROP TABLE IF EXISTS iceberg_001");
        tenv.executeSql("CREATE TABLE iceberg_001 (\n" +
//        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_001 (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");

        tenv.executeSql(
                "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");
    }
}
