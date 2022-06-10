package top.chendaye666;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import top.chendaye666.utils.MyFlatMapper;

/**
 * todo：在集群外远程提交Flink任务到TBDS Yarn集群注意事项
 *      1、在集群外节点安装 hadoop yarn zookeeper等客户端（解压包）；覆盖配置文件
 *      2、在代码中配置secureid securekey username
 *      3、在Flink高可用中配置TBDS集群Zookeeper（不然找不到资源）
 *      4、maven中添加TBDS版本依赖（hadoop_common）
 *      5、maven中添加 reference.conf 配置
 *      6、解决hadoop_common 中 commons-cli版本过低问题
 *
 * todo: 存在的问题
 *      1、Flink任务提交到TBDS Yarn上，生成的文件所有者是 yarn
 *      2、Flink独立集群运行任务。生成的文件所有者是 chenxiaolong1
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 设置 hadoop 用户
//        System.setProperty("hadoop_security_authentication_tbds_secureid", "RmgNfrMNcrwNYfQbG4ZsvJa2Va4EUzvEhaAA");
//        System.setProperty("hadoop_security_authentication_tbds_securekey", "sodFLCRlCnWnqcwHx755HH8PQCnjh1Fj");
//        System.setProperty("hadoop_security_authentication_tbds_username", "chenxiaolong1");
//        System.setProperty("HADOOP_USER_NAME", "chenxiaolong1");


        // 从文件中读取数据
//        String inputPath = "hdfs://hdfsCluster/tmp/ctestout2";
//        String outputPath = "hdfs://hdfsCluster/tmp/c_ctestuser6";
        String inputPath = args[0];
        String outputPath = args[1];
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // wc
        dataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1)
                .writeAsText(outputPath);
        // 执行
        env.execute();
    }
}
