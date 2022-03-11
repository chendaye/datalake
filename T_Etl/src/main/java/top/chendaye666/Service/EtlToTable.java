package top.chendaye666.Service;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;

public class EtlToTable {

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
    public void createOdsTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.ncdd_log (\n" +
                "    `date` STRING,\n" +
                "    `log` STRING\n" +
                ") PARTITIONED BY (`date`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
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
    public void insert(StreamTableEnvironment tEnv, SingleOutputStreamOperator<String> etl){
        // 当前时间
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd");
        String currentDaye = simpleDateFormat.format(date);
        SingleOutputStreamOperator<Tuple2<String, String>> tupleEtl = etl.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(currentDaye, s);
            }
        });

        Table tempTable = tEnv.fromDataStream(tupleEtl,$("date"), $("log")); // 流转table
        tEnv.createTemporaryView("etl_table", tempTable); // 临时表
        String sinkSql = "INSERT INTO  hadoop_prod.realtime.ncdd_log SELECT `date`, `log` FROM" +
                " default_catalog" +
                ".default_database" +
                ".etl_table";
        tEnv.executeSql(sinkSql);
    }


}
