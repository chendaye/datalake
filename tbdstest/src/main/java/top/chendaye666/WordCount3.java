package top.chendaye666;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.chendaye666.utils.MyFlatMapper;

import java.util.ArrayList;

/**
 * todo：在集群外远程提交Flink任务到TBDS Yarn集群注意事项
 *      1、在集群外节点安装 hadoop yarn zookeeper等客户端（解压包）；覆盖配置文件
 *      2、在代码中配置secureid securekey username
 *      3、在Flink高可用中配置TBDS集群Zookeeper（不然找不到资源）
 *      4、maven中添加TBDS版本依赖（hadoop_common）
 *      5、maven中添加 reference.conf 配置
 *      6、解决hadoop_common 中 commons-cli版本过低问题
 *
 *      ./bin/flink run  -m yarn-cluster -c top.chendaye666.WordCount2 -yqu root.bigdatayunwei  -ynm wc  -yjm 1024 -ytm 2048  --detached /opt/jar/tbdstest-1.0-SNAPSHOT.jar
 *
 *      ./bin/flink run -m yarn-cluster -ynm wc -yjm 2048 -ytm 1024  -d ./examples/batch/WordCount.jar --input hdfs://hdfsCluster/user/chenxiaolong1/ctest  --output hdfs://hdfsCluster/user/chenxiaolong1/ctestout
 */
public class WordCount3 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 hadoop 用户
        System.setProperty("hadoop_security_authentication_tbds_secureid", "z87A4rgCyxKQsxl1HMddJzqvnDE9qDcHn5qu");
        System.setProperty("hadoop_security_authentication_tbds_securekey", "ZQP4suI4RvIittcng5aaNhsRh0Bo6V77");
        System.setProperty("hadoop_security_authentication_tbds_username", "chenxiaolong1");
        System.setProperty("HADOOP_USER_NAME", "chenxiaolong1");

        // 从文件中读取数据
        String outputPath = "hdfs://hdfsCluster/tmp/c_ctestuser5";
        DataStreamSource<String> dataSource = env.readTextFile(outputPath);
        // wc
        dataSource.print();
        // 执行
        env.execute();
    }
}
