package top.chendaye666.Service;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.chendaye666.pojo.LogPraseEntity;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;

public class EtlToTableervice {

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
     * 新建存储etl数据的表
     * @param tEnv
     */
    public void createOdsTable(StreamTableEnvironment tEnv, String tableName){
        String sql = "CREATE TABLE IF NOT EXISTS "+tableName+" (\n" +
                "    `date` STRING,\n" +
                "    `log` STRING\n" +
                ") PARTITIONED BY (`date`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='20',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }

    /**
     * 清洗日志入库
     * @param tEnv
     * @param etl
     */
    public void insert(StreamTableEnvironment tEnv, SingleOutputStreamOperator<String> etl, String tableName){
        // 当前时间
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd");
        String currentDaye = simpleDateFormat.format(date);
        SingleOutputStreamOperator<LogPraseEntity> tupleEtl = etl.map(new MapFunction<String, LogPraseEntity>() {
            @Override
            public LogPraseEntity map(String s) throws Exception {
                return new LogPraseEntity(currentDaye, s);
            }
        });

//        tupleEtl.print("tupleEtl");
        Table tempTable = tEnv.fromDataStream(tupleEtl, Schema.newBuilder()
                .column("date", "STRING")
                .column("log", "STRING")
                .build());
        tEnv.createTemporaryView("etl_table", tempTable); // 临时表
//        tEnv.sqlQuery("select * from etl_table limit 10").execute().print();
        String sinkSql = "INSERT INTO  "+tableName+" SELECT `date`, `log` FROM" +
                " default_catalog" +
                ".default_database" +
                ".etl_table";
        tEnv.executeSql(sinkSql);
    }


}
