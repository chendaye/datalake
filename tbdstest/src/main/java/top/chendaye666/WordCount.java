package top.chendaye666;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import top.chendaye666.utils.MyFlatMapper;

/**
 * todo：在集群外远程提交Flink任务到TBDS Yarn集群注意事项
 *      1、在集群外节点安装 hadoop yarn zookeeper等客户端（解压包）；覆盖配置文件
 *      2、在代码中配置secureid securekey username
 *      3、在Flink高可用中配置TBDS集群Zookeeper（不然找不到资源）
 *      4、maven中添加TBDS版本依赖（hadoop_common）
 *      5、maven中添加 reference.conf 配置
 *      6、解决hadoop_common 中 commons-cli版本过低问题
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 设置 hadoop 用户
        System.setProperty("hadoop_security_authentication_tbds_secureid", "z87A4rgCyxKQsxl1HMddJzqvnDE9qDcHn5qu");
        System.setProperty("hadoop_security_authentication_tbds_securekey", "ZQP4suI4RvIittcng5aaNhsRh0Bo6V77");
        System.setProperty("hadoop_security_authentication_tbds_username", "chenxiaolong1");
        System.setProperty("HADOOP_USER_NAME", "chenxiaolong1");

        // 从文件中读取数据
        String inputPath = "hdfs://hdfsCluster/user/chenxiaolong1/ctest";
        String outputPath = "hdfs://hdfsCluster/user/chenxiaolong1/ctestout";
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // wc
        dataSource.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1) // // 将第二个位置上的数据求和
                .writeAsText(outputPath);
        // 执行
        env.execute();
    }
}
