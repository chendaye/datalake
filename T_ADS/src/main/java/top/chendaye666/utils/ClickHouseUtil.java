package top.chendaye666.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ClickHouseUtil {
    private static Connection connection;

    /**
     * 连接clickhouse
     * @param host
     * @param port
     * @param database
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static Connection getConnection(String host, int port, String database) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
//        Class.forName("com.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address);
        return connection;
    }

    public static Connection getConnection(String host, int port) throws SQLException,ClassNotFoundException{
        return getConnection(host, port, "default");
    }

    /**
     * 执行ddl
     * @param ddl
     * @param host
     * @param port
     */
    public static void executeDdl(String ddl, String host, int port){
        try(Connection connection = getConnection(host, port); Statement stmt = connection.createStatement()){
            // 执行ddl语句
            stmt.execute(ddl);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行ddl
     * @param ddl
     * @param host
     * @param port
     * @param database
     */
    public static void executeDdl(String ddl, String host, int port, String database){
        try(Connection connection = getConnection(host, port, database); Statement stmt = connection.createStatement()){
            // 执行ddl语句
            stmt.execute(ddl);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void  close()throws SQLException {
        connection.close();
    }
}
