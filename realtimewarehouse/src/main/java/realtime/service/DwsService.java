package realtime.service;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsService {
    public void createDwsTable(StreamTableEnvironment tEnv){
        String sql = "CREATE TABLE IF NOT EXISTS hadoop_prod.realtime.dws_ncddzt (\n" +
                "    `source_type` STRING,\n" +
                "    `agent_timestamp` STRING,\n" +
                "    `topic` STRING,\n" +
                "    `total` BIGINT\n" +
                ") PARTITIONED BY (`topic`) WITH (\n" +
                "    'write.metadata.delete-after-commit.enabled'='true',\n" +
                "    'write.metadata.previous-versions-max'='6',\n" +
                "    'read.split.target-size'='1073741824',\n" +
                "    'write.distribution-mode'='hash'\n" +
                ")";

        tEnv.executeSql(sql);
    }
}
